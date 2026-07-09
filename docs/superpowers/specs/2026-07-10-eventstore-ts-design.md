# eventstore-ts: Go イベントストアライブラリの TypeScript 移植

## Context

`github.com/Hiroshi0900/eventstore`（Go 製イベントソーシングライブラリ）と同等の TypeScript ライブラリを新規作成する。**ワイヤ完全互換**（同一 DynamoDB テーブルを Go/TS 両方から読み書き可能）が要件。ブレストで確定した方針:

- スコープ: フル移植（core + memory + DynamoDB）
- 配置: 新規別リポジトリ `~/develop/terrat/eventstore-ts`（GitHub: `Hiroshi0900/eventstore-ts`）
- パッケージ名: `@hiroshi0900/eventstore`、単一パッケージ + subpath exports（`.`, `./memory`, `./dynamodb`）
- API: TS イディオム重視（neverthrow Result、readonly、bigint）、概念は Go 版と 1:1
- ツール: pnpm + tsup (ESM+CJS dual) + vitest + biome、Node 20+
- OTel traceparent/tracestate 注入は初回から（`@opentelemetry/api` は peer dep、AWS SDK は `./dynamodb` 用 peer dep）

## ワイヤ互換仕様（Go 版から抽出済み・ビットレベル）

### DynamoDB 属性（journal イベントアイテム）
| attr | type | 値 |
|---|---|---|
| pkey | S | `{TypeName}-{shardID}` |
| skey | S | `{TypeName}-{Value}-{seqNr を 20 桁ゼロパディング}` |
| aid | S | `id.asString()` |
| seq_nr | N | 10 進 |
| payload | **B (Binary)** | EventSerializer 出力バイト列 |
| occurred_at | N | **Unix ミリ秒** |
| type_name | S | eventTypeName |
| event_id | S | 32 桁小文字 hex（crypto 16 bytes） |
| is_created | BOOL | currentSeqNr === 0 |
| traceparent / tracestate | S | **非空のときのみ書き込む** |

### snapshot アイテム
pkey / skey(`{TypeName}-{Value}-0` ※リテラル 0、パディングなし) / aid / seq_nr(N) / version(N) / payload(B) / occurred_at(N ミリ秒)。type_name, event_id, is_created, trace 系は**持たない**。

### keyresolver
- FNV-1a **64bit**（offset `0xcbf29ce484222325n`, prime `0x100000001b3n`）を `id.value()` の UTF-8 バイトに適用、`% shardCount`。各乗算後 `& 0xFFFFFFFFFFFFFFFFn` マスク。
- **shardCount <= 1 のときはハッシュせず shardID=0**（デフォルト shardCount=1）。shardCount 0 は 1 に正規化。
- skey パディング: `String(seqNr).padStart(20, "0")`（seqNr は bigint）。

### DynamoDB Store 動作
- デフォルト設定: journal / snapshot テーブル、GSI 名 `journal-aid-index` / `snapshot-aid-index`（HASH=aid S, RANGE=seq_nr N, projection ALL）、PAY_PER_REQUEST。
- `getLatestSnapshot`: GetItem（snapshot の `-0` スロット）、ConsistentRead なし。
- `getEventsSince`: seqNr が uint64 max なら即 []。journal GSI を Query、`#aid = :aid AND #seq_nr > :seqNr`（排他的下限）、ScanIndexForward=true、LastEvaluatedKey で全ページネーション。
- `persistEvent`: PutItem、条件 `attribute_not_exists(#pk)`（#pk→pkey）。ConditionalCheckFailed → DuplicateAggregateError。
- `persistEventAndSnapshot`: TransactWriteItems（journal Put + snapshot Put）。楽観ロック: `expected = snap.version - 1n`。expected===0n なら `attribute_not_exists(#version)`、それ以外は `attribute_not_exists(#version) OR #version = :expected`。TransactionCanceledException の CancellationReasons に `ConditionalCheckFailed` があれば OptimisticLockError。
- OTel: W3C propagator で traceparent/tracestate を取得しイベント属性に注入（イベントのみ）。

### Repository セマンティクス
- `newAggregate(id)`: seqNr=0n, version=0n の blank handle（DB アクセスなし）。
- `load(id)`: snapshot → あればそこから、なければ blank。`getEventsSince(seqNr)` を replay（applyEvent で seqNr++）。snapshot なし & イベント 0 件 → AggregateNotFoundError。
- `save(loaded, cmd)`: owner 検証（他 repo の handle / 未初期化 handle は InvalidAggregateError）→ applyCommand → applyEvent → nextSeqNr = current+1n → eventId 生成。
  - `isCreated = currentSeqNr === 0n`
  - `snapshotInterval`（デフォルト 5、0 なら snapshot なし）: `nextSeqNr % interval === 0n` なら version+1 で persistEventAndSnapshot、それ以外は persistEvent（expectedVersion = currentVersion、ただし `currentSeqNr > 0 && expectedVersion === 0` なら expectedVersion = currentSeqNr の特例あり）。
- Go 版の reflect による value-semantics guard は**移植しない**（ワイヤ影響なし。immutability は applyEvent が新オブジェクトを返す規約 + Readonly 型で担保）。

### memory store
`id.asString()` キーの Map。getEventsSince は seqNr フィルタ + 昇順ソート。persistEvent: expectedVersion===0n かつ既存イベントあり → Duplicate、seqNr 重複 → Duplicate。persistEventAndSnapshot: 既存 snapshot version（なければ 0n）!== expected → OptimisticLock。

## 実装計画

### リポジトリ構成
```
~/develop/terrat/eventstore-ts/
├── src/
│   ├── index.ts            # core re-export
│   ├── types.ts            # AggregateId, Aggregate<C,E>, Command, Event, StoredEvent<E>, StoredSnapshot<A>
│   ├── errors.ts           # AggregateNotFoundError / OptimisticLockError / DuplicateAggregateError / InvalidAggregateError（class + kind タグ）
│   ├── event-id.ts         # generateEventId(): 32 桁 hex
│   ├── config.ts           # EventStoreConfig（snapshotInterval, shouldSnapshot）
│   ├── serializer.ts       # AggregateSerializer / EventSerializer インターフェース + JSON 実装
│   ├── event-store.ts      # EventStore<A,C,E> インターフェース
│   ├── repository.ts       # createRepository + LoadedAggregate（owner 検証付き不透明 handle）
│   ├── memory/store.ts
│   ├── dynamodb/store.ts
│   ├── dynamodb/key-resolver.ts
│   └── dynamodb/otel.ts
├── test/                   # vitest。Go 版テストをミラー
├── package.json（exports: ".", "./memory", "./dynamodb"; peerDeps: @aws-sdk/client-dynamodb(optional), @opentelemetry/api(optional); deps: neverthrow）
├── tsup.config.ts, tsconfig.json, biome.json, .github/workflows/ci.yml
└── docs/superpowers/specs/2026-07-10-eventstore-ts-design.md（本設計を spec として保存）
```

### API 形状（コア）
```ts
interface AggregateId { typeName(): string; value(): string; asString(): string; }
interface Aggregate<C, E> {
  aggregateId(): AggregateId;
  applyCommand(cmd: C): Result<E, Error>;
  applyEvent(event: E): Aggregate<C, E>;
}
const repo = createRepository<A, C, E>({ store, createBlank, config });
repo.newAggregate(id): LoadedAggregate<A, C, E>
repo.load(id): ResultAsync<LoadedAggregate<A,C,E>, RepositoryError>
repo.save(loaded, cmd): ResultAsync<LoadedAggregate<A,C,E>, RepositoryError>
loaded.aggregate(): A   // handle が公開するのはこれのみ
```
seqNr / version は全レイヤーで `bigint`。

### 実装ステップ（TDD、Go 版テストを移植しながら）
1. リポジトリ scaffold（pnpm init, tsup, vitest, biome, tsconfig, CI）+ 設計 spec ドキュメントをコミット
2. core 型・エラー・eventId・config（eventId regex テスト、shouldSnapshot テーブルテスト移植）
3. memory store（Go memory/store_test 移植）
4. repository + LoadedAggregate（Go repository_test の主要シナリオ移植: 初回 save、連鎖 save、replay、snapshot 境界、楽観ロック、foreign handle 拒否）
5. dynamodb/key-resolver（Go keyresolver_test の期待値文字列を完全一致で移植 + FNV-1a 既知値フィクスチャは Go 側で `go run` して生成）
6. dynamodb/store（fake DynamoDB クライアントで QueryInput・条件式・Transact・ページネーションを検証。Go dynamodb/store_test をミラー）
7. OTel 注入（traceparent 非空時のみ属性追加のテスト）
8. README（Go 版 CLAUDE.md ベースの使い方 + 集約実装手順）、CHANGELOG、GitHub リポジトリ作成 & push

### 検証
- `pnpm test`（vitest 全通過）、`pnpm build`（tsup ESM+CJS+d.ts）、`pnpm lint`（biome）
- 互換性の確証: keyresolver / envelope 属性のスナップショットテストが Go 版の期待値文字列・数値と一致すること
- （任意・推奨）dynamodb-local を docker で立て、Go 版で書いたアイテムを TS 版で load して集約が復元できる E2E を 1 本（実装後にユーザーと相談）

## 未決事項（実装中に確認）
- GitHub リポジトリ作成のタイミング（scaffold 直後に `gh repo create` するか、ローカル完成後か）
- npm publish は今回スコープ外（リポジトリ完成まで）
