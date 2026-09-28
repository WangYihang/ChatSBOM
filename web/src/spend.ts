/**
 * The daily spend cap's counter (#33): one Durable Object per UTC day.
 *
 * The cap was a running total in KV, checked before a call and added to
 * after it by reading the total and writing it back. KV reads can be a
 * minute stale, and of two writes to one key one is kept, so the check
 * admitted whatever arrived together and the total lost whatever was
 * added together: 20 questions at once against a $5 cap were all
 * admitted, about $44 of calls, of which $2.20 was recorded.
 *
 * A Durable Object is one instance with storage of its own, taking one
 * call at a time. So the check and the addition are one step here, and
 * the step comes before the model call, not after it:
 *
 *   - `reserve` holds the call's worst case against the cap, or refuses.
 *     Whatever arrives together, what is spent and held never passes
 *     the cap, so neither can what is paid.
 *   - `settle` replaces the worst case with what the call cost, once
 *     the answer says.
 *   - `refund` releases it, for a call the API refused.
 *
 * One object per day, named for it: a day starts at nothing by being a
 * new object, and a call reserved before midnight settles against the
 * day that admitted it. Nothing removes a past day's object: deployed,
 * it is a few hundred bytes of storage; under `wrangler dev`, a small
 * SQLite file in .wrangler/state/v3/do/chatsbom-SpendCounter.
 */
import { DurableObject } from 'cloudflare:workers';

/** A day's spending: settled, and held for calls in flight. */
interface Ledger {
  /** Dollars that answered calls cost. */
  spent: number;
  /** Dollars held for each call in flight, by reservation. */
  held: Map<string, number>;
}

/** The one key the ledger is stored under. */
const LEDGER = 'ledger';

export class SpendCounter extends DurableObject<unknown> {
  private ledger: Ledger = { spent: 0, held: new Map() };

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    // Nothing is answered until the day's ledger is loaded.
    void ctx.blockConcurrencyWhile(async () => {
      this.ledger = (await ctx.storage.get<Ledger>(LEDGER)) ?? this.ledger;
    });
  }

  /**
   * Hold `usd` against `cap` for the call `id` names, or refuse.
   *
   * Nothing is awaited between the check and the hold, and the object
   * takes one call at a time, so no reservation can come between them.
   * The comparisons are written so that one that is not a number
   * refuses: a cap that compares false would admit anything. Asked
   * again for a reservation it holds, it says so and holds no more.
   */
  reserve(id: string, usd: number, cap: number): boolean {
    if (this.ledger.held.has(id)) return true;
    if (!(usd >= 0) || !(committed(this.ledger) + usd <= cap)) return false;
    this.ledger.held.set(id, usd);
    this.save();
    return true;
  }

  /**
   * Replace a reservation with what its call cost.
   *
   * A cost that cannot be read keeps the worst case, which is known to
   * be enough. Settling what is not held — twice, or after a refund —
   * changes nothing.
   */
  settle(id: string, usd: number): void {
    const held = this.ledger.held.get(id);
    if (held === undefined) return;
    this.ledger.held.delete(id);
    this.ledger.spent += usd >= 0 ? usd : held;
    this.save();
  }

  /** Release a reservation whose call the API refused, and so did not bill. */
  refund(id: string): void {
    if (this.ledger.held.delete(id)) this.save();
  }

  /** The day's spend, and what is held for calls in flight. */
  usage(): { spent: number; held: number } {
    return { spent: this.ledger.spent, held: sum(this.ledger.held) };
  }

  private save(): void {
    // Not awaited: the runtime holds this call's answer until the write
    // is durable, and nothing reads the stored copy but the next start.
    void this.ctx.storage.put(LEDGER, this.ledger);
  }
}

/** What a new reservation has to fit beside. */
function committed(ledger: Ledger): number {
  return ledger.spent + sum(ledger.held);
}

function sum(amounts: Map<string, number>): number {
  let total = 0;
  for (const usd of amounts.values()) total += usd;
  return total;
}
