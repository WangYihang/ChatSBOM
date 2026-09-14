/**
 * Serves the dashboard and streams the Parquet dataset out of R2.
 *
 * Two serving models, deliberately both present.
 *
 * `/api/q` answers from D1: the browser names a method, the Worker runs
 * the statement. That is what the dashboard uses — it costs the visitor
 * about 110 KB rather than the 28 MB a browser-side engine needed.
 *
 * `/data/*` still streams Parquet byte ranges out of R2, for anyone
 * consuming the dataset directly. Keeping it means the export stays
 * useful outside this page, and the serving model can be reversed
 * without a flag day.
 */
import type { ChatEnv } from './chat';
import { handleChat } from './chat';
import { handleQuery } from './d1/api';
import { DATA_FILES, SCHEMA_VERSION } from './schema';

export interface Env extends ChatEnv {
  ASSETS: Fetcher;
  DATA: R2Bucket;
  /**
   * The dataset as a database.
   *
   * Optional while both serving models coexist: /data/* still streams
   * Parquet for the browser-side engine, and /api/q answers from D1.
   * A deployment configures whichever it uses.
   */
  DB?: D1Database;
}

/**
 * Whether a key under /data/ is one this deployment serves.
 *
 * Shape rather than a list: table files are content-addressed —
 * `repositories-659592a2.parquet` — because they are served
 * `immutable`, and a fixed name made that a lie (the next export reused
 * the URL while clients kept the old bytes for a year). The guarantee
 * that mattered is preserved: only a known table with a well-formed
 * hash, and no path separators or traversal.
 *
 * `manifest.json` is exempt from the hash. It is the entry point, so
 * its URL has to be stable to be found, which is also why it alone is
 * served with `must-revalidate`.
 */
const TABLE_NAMES = Object.keys(DATA_FILES);
const ADDRESSED = /^([a-z_]+)-[0-9a-f]{8}\.parquet$/;

export function isServableData(key: string): boolean {
  if (key === MANIFEST) return true;
  const match = ADDRESSED.exec(key);
  return match !== null && TABLE_NAMES.includes(match[1]!);
}

const MANIFEST = 'manifest.json';


/** Parquet files are replaced, never edited, so they cache indefinitely. */
const IMMUTABLE = 'public, max-age=31536000, immutable';
/** The manifest names the current files, so it must be revalidated. */
const REVALIDATE = 'public, max-age=60, must-revalidate';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname.startsWith('/data/')) {
      return serveData(request, env, url.pathname.slice('/data/'.length));
    }

    if (url.pathname === '/api/q') {
      if (!env.DB) {
        return new Response(
          JSON.stringify({ error: 'This deployment has no database bound.' }),
          { status: 503, headers: { 'content-type': 'application/json' } },
        );
      }
      return handleQuery(request, { DB: env.DB });
    }

    if (url.pathname === '/api/chat') {
      return handleChat(request, env);
    }

    return env.ASSETS.fetch(request);
  },
} satisfies ExportedHandler<Env>;

async function serveData(
  request: Request,
  env: Env,
  key: string,
  servable: (key: string) => boolean = isServableData,
): Promise<Response> {
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    return new Response('Method not allowed', {
      status: 405,
      headers: { Allow: 'GET, HEAD' },
    });
  }

  if (!servable(key)) {
    return new Response('Not found', { status: 404 });
  }

  const range = request.headers.get('range');
  const parsed = range ? parseRange(range) : null;
  if (range && !parsed) {
    return new Response('Malformed Range header', { status: 400 });
  }

  const object = await env.DATA.get(key, {
    ...(parsed ? { range: parsed } : {}),
    onlyIf: request.headers,
  });

  if (object === null) {
    return new Response('Not found', { status: 404 });
  }

  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set('etag', object.httpEtag);
  headers.set('accept-ranges', 'bytes');
  headers.set('x-schema-version', SCHEMA_VERSION);
  headers.set(
    'cache-control',
    key === MANIFEST ? REVALIDATE : IMMUTABLE,
  );
  // A Parquet reader fetching ranges needs the size, not just the
  // promise of range support — see the note on the HEAD reply below.
  headers.set('access-control-allow-origin', '*');
  headers.set(
    'access-control-expose-headers',
    'content-length, content-range, accept-ranges, etag',
  );

  // A conditional request that matched has no body to return.
  if (!('body' in object)) {
    return new Response(null, { status: 304, headers });
  }

  // Only a client that asked for a range gets a 206. R2's local
  // simulator populates `object.range` even for a full get, so keying
  // off that alone answers plain GETs with a partial response.
  if (parsed && object.range && 'offset' in object.range) {
    const offset = object.range.offset ?? 0;
    const length = object.range.length ?? object.size - offset;
    headers.set(
      'content-range',
      `bytes ${offset}-${offset + length - 1}/${object.size}`,
    );
    return new Response(request.method === 'HEAD' ? null : object.body, {
      status: 206,
      headers,
    });
  }

  if (request.method === 'HEAD') {
    // The size is the whole point of the probe.
    //
    // A Parquet reader sends HEAD before reading, because a
    // Parquet footer is located from the *end* — without a length there
    // is no offset to ask for, so the engine gives up on ranged reads
    // and downloads the file whole. A body-less Response gets no
    // automatic `content-length`, and `writeHttpMetadata` does not add
    // one, so ours advertised `Accept-Ranges: bytes` and no size.
    //
    // Measured before: 36 requests, 0 ranged, 16,651,228 bytes for
    // artifacts alone and 28 MB on a first load.
    headers.set('content-length', String(object.size));
    return new Response(null, { headers });
  }

  return new Response(object.body, { headers });
}

/**
 * Parse a single-range `Range: bytes=...` header into R2's range shape.
 *
 * Only one range is supported; a multipart response would defeat the
 * point, and a Parquet reader asks for one range at a time.
 */
export function parseRange(header: string): R2Range | null {
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match) return null;

  const [, rawStart, rawEnd] = match;
  const hasStart = rawStart !== '';
  const hasEnd = rawEnd !== '';

  if (!hasStart && !hasEnd) return null;

  if (!hasStart) {
    // `bytes=-N`: the final N bytes, which is how Parquet readers find
    // the footer before they know the file length.
    const suffix = Number(rawEnd);
    return suffix > 0 ? { suffix } : null;
  }

  const offset = Number(rawStart);
  if (!hasEnd) return { offset };

  const end = Number(rawEnd);
  if (end < offset) return null;
  return { offset, length: end - offset + 1 };
}
