/**
 * Adapts a Cloudflare D1 binding to the query layer's interface.
 *
 * Separate from queries.ts so that file stays free of Workers runtime
 * types. The browser's client imports result *types* from it, and a
 * `D1Database` reference there would drag the Workers environment into
 * a browser compile.
 */
import type { D1Queryable } from './queries';

export class D1Binding implements D1Queryable {
  constructor(private readonly database: D1Database) {}

  async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    const statement = this.database.prepare(sql);
    const bound = params.length ? statement.bind(...params) : statement;
    const { results } = await bound.all<T>();
    return results ?? [];
  }
}
