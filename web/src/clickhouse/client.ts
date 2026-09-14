/**
 * The ClickHouse HTTP interface, as much of it as this Worker needs.
 *
 * One method: `rows(sql, params)`. Everything else here exists to make
 * that method safe and its failures legible.
 *
 * **Values are bound, never interpolated.** ClickHouse takes parameters
 * out of band — `{name:Type}` in the statement, `param_name=value` in
 * the query string — and substitutes them as typed values, not as text.
 * Verified against the running server: a parameter of
 * `x'; DROP TABLE artifacts;--` came back as a count of zero with the
 * table intact. This page is public and the alternative is a page that
 * can compose any SQL, so there is no escaping helper here on purpose:
 * if a caller cannot express something through a parameter, the
 * statement is wrong.
 *
 * **The account is read-only.** `guest` carries `readonly=1`, no DDL,
 * and cost ceilings the server enforces — 30 s, 4 GB, 2e9 rows read,
 * 16 concurrent queries per user. So a statement that gets through and
 * is expensive fails as a query rather than as a server.
 */

/** What a ClickHouse `FORMAT JSON` response carries. */
interface JsonResponse<T> {
  data: T[];
  rows?: number;
  statistics?: { elapsed: number; rows_read: number; bytes_read: number };
  exception?: string;
}

/** A bound parameter. Strings, numbers and booleans only. */
export type Param = string | number | boolean;

export class ClickHouseError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message);
    this.name = 'ClickHouseError';
  }
}

export interface ClickHouseConfig {
  /** Origin of the HTTP interface, e.g. `http://127.0.0.1:8123`. */
  url: string;
  database: string;
  user: string;
  password: string;
  /**
   * Per-request deadline.
   *
   * Shorter than the server's own 30 s cap, because a Worker holding a
   * request open for thirty seconds has already lost the reader. The
   * server's cap is the backstop for cost; this is the one for
   * latency.
   */
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 10_000;

export class ClickHouse {
  constructor(private readonly config: ClickHouseConfig) {}

  /**
   * Run one statement and return its rows.
   *
   * `FORMAT JSON` is appended here rather than written by callers, so
   * no query can choose a format that changes the shape this returns.
   */
  async rows<T>(sql: string, params: Record<string, Param> = {}): Promise<T[]> {
    const query = new URLSearchParams({
      database: this.config.database,
      // Defence in depth behind the read-only account: a statement that
      // somehow mutated would be refused twice. This one a read-only
      // account may send, because it cannot relax anything.
      readonly: '1',
      // No cost settings here on purpose. A `readonly=1` account cannot
      // modify them — `max_execution_time=25` came back as
      // `Cannot modify 'max_execution_time' setting in readonly mode`
      // and every query 500'd — and it does not need to: the server's
      // own `guest_readonly` profile already enforces 30 s, 100,000
      // result rows and 2e9 rows read. Ceilings belong on the account,
      // where a caller cannot raise them, rather than on the request,
      // where this one cannot even lower them.
    });
    for (const [name, value] of Object.entries(params)) {
      query.set(`param_${name}`, String(value));
    }

    const controller = new AbortController();
    const deadline = setTimeout(
      () => controller.abort(),
      this.config.timeoutMs ?? DEFAULT_TIMEOUT_MS,
    );

    let response: Response;
    try {
      response = await fetch(`${this.config.url}/?${query}`, {
        method: 'POST',
        headers: {
          authorization: `Basic ${btoa(
            `${this.config.user}:${this.config.password}`,
          )}`,
          'content-type': 'text/plain; charset=utf-8',
        },
        body: `${sql}\nFORMAT JSON`,
        signal: controller.signal,
      });
    } catch (error) {
      // An abort and a refused connection are the same thing to a
      // reader — the data is not answering — but not to whoever has to
      // fix it, so they are distinguished in the message that gets
      // logged. Neither reaches the browser; `api.ts` replaces it.
      throw new ClickHouseError(
        controller.signal.aborted
          ? `Query exceeded ${this.config.timeoutMs ?? DEFAULT_TIMEOUT_MS}ms`
          : `Could not reach ClickHouse at ${this.config.url}: ${String(error)}`,
      );
    } finally {
      clearTimeout(deadline);
    }

    const text = await response.text();
    if (!response.ok) {
      // ClickHouse puts its own diagnostics in the body, and they name
      // tables and columns. Kept for the log, never returned.
      throw new ClickHouseError(
        `ClickHouse ${response.status}: ${text.slice(0, 400)}`,
        response.status,
      );
    }

    let payload: JsonResponse<T>;
    try {
      payload = JSON.parse(text) as JsonResponse<T>;
    } catch {
      throw new ClickHouseError(
        `Unparsable response: ${text.slice(0, 200)}`,
        response.status,
      );
    }

    // A 200 with an `exception` field happens when the failure arrives
    // after the headers — a long scan that trips a limit mid-stream.
    // Treating it as success returns a truncated answer as a complete
    // one.
    if (payload.exception) {
      throw new ClickHouseError(payload.exception.slice(0, 400), 200);
    }
    return payload.data ?? [];
  }

  /** The first row, or undefined. For the one-row questions. */
  async row<T>(
    sql: string,
    params: Record<string, Param> = {},
  ): Promise<T | undefined> {
    return (await this.rows<T>(sql, params))[0];
  }
}
