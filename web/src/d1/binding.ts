/**
 * Adapts a Cloudflare D1 binding to the query layer's interface.
 *
 * Separate from queries.ts so that file stays free of Workers runtime
 * types: the contract suite runs it against SQLite in Node, where a
 * `D1Database` reference would have nothing to be. (The browser reads
 * result types from `dataset/types.ts`, which holds nothing else.)
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
