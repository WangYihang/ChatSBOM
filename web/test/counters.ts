/**
 * Durable Object spend counters (#33), run in Node for the tests.
 *
 * The objects are the real `SpendCounter`; what stands in is the
 * runtime around them: storage that copies what it is given, as the
 * runtime's serializes it, and writes it a moment later, in the order
 * it was asked to; an alarm, which is written the same way (#115); and
 * a stub that reaches an object as RPC does — asynchronously, and only
 * once the object has loaded what it stored.
 */
import { SpendCounter } from '../src/spend';

/** What a Durable Object keeps across a restart: its storage, and its alarm. */
export interface Durable {
  stored: Map<string, unknown>;
  /** When the alarm is set for, in milliseconds; null for none. */
  alarm: number | null;
  /** Every write asked for so far, landed. */
  landed: Promise<void>;
}

export function durable(): Durable {
  return { stored: new Map(), alarm: null, landed: Promise.resolve() };
}

/** The day a counter is named for when a test does not say. */
export const A_DAY = '2026-09-14';

/**
 * A Durable Object's state, as much of it as a counter uses, over what
 * `kept` holds; named `name`, as `getByName` names an object, or null
 * for an object reached without a name.
 */
export function fakeState(kept: Durable = durable(), name: string | null = A_DAY) {
  const loading: Promise<unknown>[] = [];
  /** Applied a moment later, after every write asked for before it. */
  const write = (apply: () => void): Promise<void> => {
    kept.landed = kept.landed
      .then(() => new Promise((resolve) => setTimeout(resolve, 1)))
      .then(apply);
    return kept.landed;
  };
  const state = {
    id: name === null ? {} : { name },
    storage: {
      get: async (key: string) => structuredClone(kept.stored.get(key)),
      put: (key: string, value: unknown) => {
        const copy = structuredClone(value);
        return write(() => void kept.stored.set(key, copy));
      },
      getAlarm: async () => kept.alarm,
      setAlarm: (at: number | Date) => write(() => void (kept.alarm = Number(at))),
      deleteAll: () => write(() => kept.stored.clear()),
    },
    blockConcurrencyWhile: <T>(load: () => Promise<T>): Promise<T> => {
      const loaded = load();
      loading.push(loaded);
      return loaded;
    },
  };
  return {
    state: state as unknown as DurableObjectState,
    storage: state.storage,
    kept,
    loaded: () => Promise.all(loading),
  };
}

/** A counter over `kept`, named for `day`, once it has loaded. */
export async function counterOver(kept: Durable = durable(), day: string | null = A_DAY) {
  const { state, loaded } = fakeState(kept, day);
  const counter = new SpendCounter(state, {});
  await loaded();
  return counter;
}

/**
 * A namespace of counters, one per name, reached through stubs; and
 * each day's counter, for a test to read or set up.
 */
export function counters() {
  const days = new Map<string, Promise<SpendCounter>>();
  const counter = (day: string): Promise<SpendCounter> => {
    if (!days.has(day)) days.set(day, counterOver(durable(), day));
    return days.get(day)!;
  };
  const stub = (day: string) =>
    new Proxy(
      {},
      {
        get: (_, method: string) =>
          async (...args: unknown[]) => {
            const object = (await counter(day)) as unknown as Record<
              string,
              (...args: unknown[]) => unknown
            >;
            return object[method]!(...args);
          },
      },
    );
  return {
    namespace: { getByName: stub } as unknown as DurableObjectNamespace<SpendCounter>,
    counter,
    days,
  };
}
