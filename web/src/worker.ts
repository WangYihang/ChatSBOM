/**
 * Serves the dashboard and answers its questions.
 *
 * Two routes and a static-asset fallback. That is the whole surface:
 *
 *   /api/q      the dataset, queried — the browser names a method.
 *               Answered by whichever store is configured: ClickHouse
 *               over HTTP for a deployment reading live data, D1 for
 *               one shipping a snapshot.
 *   /api/chat   one model turn, relayed to the Messages API
 *   everything else   the SPA, straight from static assets
 *
 * There used to be a third: `/data/*` streamed ranged Parquet out of R2
 * for a query engine running in the browser. It is gone, and what went
 * with it is worth recording, because each piece existed for a reason
 * that no longer applies:
 *
 *   - content-addressed filenames, so `immutable` caching was honest;
 *   - `Content-Length` on HEAD, because a Parquet footer is located
 *     from the end of the file and a reader without a length downloads
 *     the whole thing;
 *   - a manifest with a checksum per file, so a reader could tell stale
 *     data from wrong data.
 *
 * None of those questions can arise now: the client holds no copy of
 * the data. `chatsbom export parquet` still exists for anyone consuming
 * the dataset outside this page, but the Worker no longer serves it.
 */
import type { ChatEnv } from './chat';
import { handleChat } from './chat';
import { handleQuery, type QueryEnv } from './d1/api';

/**
 * The daily spend cap's counter (#33). A Durable Object's class is found
 * among the Worker's exports by the name its binding in wrangler.jsonc
 * gives, so it is exported from here, the Worker's entry.
 */
export { SpendCounter } from './spend';

export interface Env extends ChatEnv, QueryEnv {
  ASSETS: Fetcher;
}

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/api/q') {
      // The 503 for an unconfigured deployment lives in `handleQuery`
      // now, because which bindings count as configured is its
      // decision: D1 and ClickHouse are both optional and either one
      // is enough. `ctx` so an answer is kept after it is sent (#42).
      return handleQuery(request, env, ctx);
    }

    if (url.pathname === '/api/chat') {
      // `ctx` so the spend is settled after the answer is sent, and a
      // failure to settle it cannot take the answer with it.
      return handleChat(request, env, ctx);
    }

    return env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;

function json(payload: unknown, status: number): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    },
  });
}
