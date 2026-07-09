import { type Result, err, ok } from "neverthrow";
import { SerializationError } from "./errors.js";

export interface AggregateSerializer<A> {
  serialize(aggregate: A): Result<Uint8Array, SerializationError>;
  deserialize(data: Uint8Array): Result<A, SerializationError>;
}

export interface EventSerializer<E> {
  serialize(event: E): Result<Uint8Array, SerializationError>;
  deserialize(typeName: string, data: Uint8Array): Result<E, SerializationError>;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Builds a serializer pair from plain encode/decode functions, mirroring the
 * Go codec adapter. Encode returns a JSON-serializable value; decode rebuilds
 * the domain object from the parsed JSON.
 */
export function jsonAggregateSerializer<A>(
  encode: (aggregate: A) => unknown,
  decode: (data: unknown) => A,
): AggregateSerializer<A> {
  return {
    serialize(aggregate) {
      try {
        return ok(textEncoder.encode(JSON.stringify(encode(aggregate))));
      } catch (cause) {
        return err(new SerializationError("failed to serialize aggregate", cause));
      }
    },
    deserialize(data) {
      try {
        return ok(decode(JSON.parse(textDecoder.decode(data))));
      } catch (cause) {
        return err(new SerializationError("failed to deserialize aggregate", cause));
      }
    },
  };
}

export function jsonEventSerializer<E>(
  encode: (event: E) => unknown,
  decode: (typeName: string, data: unknown) => E,
): EventSerializer<E> {
  return {
    serialize(event) {
      try {
        return ok(textEncoder.encode(JSON.stringify(encode(event))));
      } catch (cause) {
        return err(new SerializationError("failed to serialize event", cause));
      }
    },
    deserialize(typeName, data) {
      try {
        return ok(decode(typeName, JSON.parse(textDecoder.decode(data))));
      } catch (cause) {
        return err(new SerializationError("failed to deserialize event", cause));
      }
    },
  };
}
