import { type Result, err, ok } from "neverthrow";
import {
  type Aggregate,
  type AggregateId,
  type Command,
  type Event,
  jsonAggregateSerializer,
  jsonEventSerializer,
  newAggregateId,
} from "../src/index.js";

export function counterId(value: string): AggregateId {
  return newAggregateId("Counter", value);
}

export class IncrementCommand implements Command {
  constructor(readonly amount: number) {}
  commandTypeName(): string {
    return "Increment";
  }
}

export class FailingCommand implements Command {
  commandTypeName(): string {
    return "Failing";
  }
}

export type CounterCommand = IncrementCommand | FailingCommand;

export class IncrementedEvent implements Event {
  constructor(
    private readonly id: AggregateId,
    readonly amount: number,
  ) {}
  eventTypeName(): string {
    return "Incremented";
  }
  aggregateId(): AggregateId {
    return this.id;
  }
}

export type CounterEvent = IncrementedEvent;

export class Counter implements Aggregate<CounterCommand, CounterEvent> {
  constructor(
    private readonly id: AggregateId,
    readonly count: number,
  ) {}

  aggregateId(): AggregateId {
    return this.id;
  }

  applyCommand(cmd: CounterCommand): Result<CounterEvent, Error> {
    if (cmd instanceof FailingCommand) {
      return err(new Error("command rejected"));
    }
    return ok(new IncrementedEvent(this.id, cmd.amount));
  }

  applyEvent(event: CounterEvent): Aggregate<CounterCommand, CounterEvent> {
    return new Counter(this.id, this.count + event.amount);
  }
}

export function createBlankCounter(id: AggregateId): Counter {
  return new Counter(id, 0);
}

export const counterAggregateSerializer = jsonAggregateSerializer<Counter>(
  (counter) => ({ id: counter.aggregateId().value(), count: counter.count }),
  (data) => {
    const { id, count } = data as { id: string; count: number };
    return new Counter(counterId(id), count);
  },
);

export const counterEventSerializer = jsonEventSerializer<CounterEvent>(
  (event) => ({ id: event.aggregateId().value(), amount: event.amount }),
  (_typeName, data) => {
    const { id, amount } = data as { id: string; amount: number };
    return new IncrementedEvent(counterId(id), amount);
  },
);
