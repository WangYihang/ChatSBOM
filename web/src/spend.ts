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
 * day that admitted it.
 *
 * And each clears itself once its day is over (#115). Nothing removed
 * a past day's object, so every day kept its storage for good: under
 * `wrangler dev` about 86 KB a day in .wrangler/state/v3/do/
 * chatsbom-SpendCounter. With its first write a counter sets an alarm
 * for an hour after its day ends, when nothing can reserve against it
 * and the last call reserved before midnight has long settled, and the
 * alarm deletes everything it stored. Deployed, that frees the object
 * altogether; `wrangler dev` keeps an empty 4 KB file.
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

const DAY_MS = 24 * 60 * 60 * 1000;

/**
 * How long a counter outlives its day. A call reserved just before
 * midnight settles against that day, and a call is one model turn, which
 * the SDK gives up on after ten minutes.
 */
const KEPT_PAST_ITS_DAY_MS = 60 * 60 * 1000;

export class SpendCounter extends DurableObject<unknown> {
  private ledger: Ledger = { spent: 0, held: new Map() };
  /** Whether an alarm is set to clear the day once it is over. */
  private clearing = false;

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    // Nothing is answered until the day's ledger is loaded.
    void ctx.blockConcurrencyWhile(async () => {
      this.ledger = (await ctx.storage.get<Ledger>(LEDGER)) ?? this.ledger;
      this.clearing = (await ctx.storage.getAlarm()) !== null;
    });
  }

  /**
   * The day is over: forget it (#115). Nothing reserves against a past
   * day, and what it spent was only ever read to keep the cap that day.
   * A hold still in place is a call lost on the way, which may have been
   * billed; the day it counted against is over all the same.
   */
  override async alarm(): Promise<void> {
    await this.ctx.storage.deleteAll();
    this.ledger = { spent: 0, held: new Map() };
    this.clearing = false;
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
    if (!this.clearing) this.clearOnceOver();
  }

  /**
   * Have the day cleared once it is over: named for its day, as every
   * counter is (`spendDay`), the object can say when that is. One that
   * cannot keeps its day, since clearing a day still being counted would
   * lift the cap for the rest of it.
   */
  private clearOnceOver(): void {
    const at = clearedAt(this.ctx.id.name);
    if (at === null) return;
    this.clearing = true;
    void this.ctx.storage.setAlarm(at);
  }
}

/** When the UTC day `name` names has been over for the margin; null if it names none. */
function clearedAt(name: string | undefined): number | null {
  if (name === undefined || !/^\d{4}-\d{2}-\d{2}$/.test(name)) return null;
  const start = Date.parse(`${name}T00:00:00Z`);
  return Number.isNaN(start) ? null : start + DAY_MS + KEPT_PAST_ITS_DAY_MS;
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
