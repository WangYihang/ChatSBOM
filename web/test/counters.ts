/**
 * Durable Object spend counters (#33), run in Node for the tests.
 *
 * The objects are the real `SpendCounter`; what stands in is the
 * runtime around them: storage that copies what it is given, as the
 * runtime's serializes it, and writes it a moment later, and a stub
 * that reaches an object as RPC does — asynchronously, and only once
 * the object has loaded what it stored.
 */
import { SpendCounter } from '../src/spend';

/** A Durable Object's state, as much of it as a counter uses. */
export function fakeState(stored = new Map<string, unknown>()) {
  const loading: Promise<unknown>[] = [];
  const state = {
    storage: {
      get: async (key: string) => structuredClone(stored.get(key)),
      put: async (key: string, value: unknown) => {
        const copy = structuredClone(value);
        await new Promise((resolve) => setTimeout(resolve, 1));
        stored.set(key, copy);
      },
    },
    blockConcurrencyWhile: <T>(load: () => Promise<T>): Promise<T> => {
      const loaded = load();
      loading.push(loaded);
      return loaded;
    },
  };
  return {
    state: state as unknown as DurableObjectState,
    stored,
    loaded: () => Promise.all(loading),
  };
}

/** A counter over `stored`, once it has loaded. */
export async function counterOver(stored = new Map<string, unknown>()) {
  const { state, loaded } = fakeState(stored);
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
    if (!days.has(day)) days.set(day, counterOver());
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
