/**
 * `cloudflare:workers`, for the tests that run in Node (#33).
 *
 * The Workers runtime provides that module and Node has no such thing,
 * so vitest.config.ts resolves it here. The one export the Worker takes
 * from it is the base class of a Durable Object, which does no more
 * than keep the state and environment it is built with; the tests
 * construct a counter with state of their own (test/counters.ts).
 */
export class DurableObject<Env = unknown> {
  constructor(
    protected readonly ctx: DurableObjectState,
    protected readonly env: Env,
  ) {}
}
