/**
 * What this page is actually showing.
 *
 * Four questions that come up the moment a number looks wrong, and none
 * of which should need a database to answer: which build produced the
 * dataset, which export this browser is looking at, how fresh the rows
 * are, and how big each file was.
 *
 * The checksum prefix earns its place. Parquet is served immutable, so a
 * browser can hold an old copy indefinitely; seeing the digest is how
 * you tell "the data is wrong" from "this tab has stale data".
 *
 * The freshness span is deliberately two dates rather than one. A single
 * "updated at" invites the reader to assume the whole corpus is that
 * age, and here the ends are seven months apart.
 */
import type { Manifest } from '../duckdb';

export function Metadata({ manifest }: { manifest: Manifest }) {
  const { observedFrom, observedTo } = manifest.freshness ?? {};

  return (
    <div className="panel">
      <h2>
        Dataset metadata
        <span className="qual">for debugging what you are looking at</span>
      </h2>
      <p className="note">
        Observation dates are when <em>this</em> pipeline recorded a
        repository&rsquo;s dependencies. Star counts and push dates come
        from the repository metadata collected earlier and are not
        refreshed by a dependency rescan, so a row can legitimately show a
        recent scan beside an older push.
      </p>

      <dl className="meta">
        <div>
          <dt>Generator</dt>
          <dd className="mono">{manifest.generator}</dd>
        </div>
        <div>
          <dt>Export schema</dt>
          <dd className="mono">v{manifest.schemaVersion}</dd>
        </div>
        <div>
          <dt>Observed</dt>
          <dd className="mono">
            {observedFrom && observedTo
              ? `${observedFrom} → ${observedTo}`
              : 'unknown'}
          </dd>
        </div>
      </dl>

      <div className="tablewrap">
        <table>
          <thead>
            <tr>
              <th scope="col">File</th>
              <th scope="col" className="num">
                Rows
              </th>
              <th scope="col" className="num">
                Size
              </th>
              <th scope="col">SHA-256</th>
            </tr>
          </thead>
          <tbody>
            {manifest.files.map((file) => (
              <tr key={file.name}>
                <td className="mono">{file.name}</td>
                <td className="num">
                  {rowsFor(manifest, file.name)?.toLocaleString() ?? '—'}
                </td>
                <td className="num">{megabytes(file.bytes)}</td>
                {/* A prefix is enough to tell two exports apart, and a
                    full digest crowds out the columns that get read. */}
                <td className="mono">{file.sha256.slice(0, 8)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/**
 * Row counts are keyed by table; files by content-addressed filename.
 *
 * Both the hash and the extension have to come off: stripping only
 * `.parquet` leaves `artifacts-12e8dd23`, which is not a row-count key,
 * and every row showed a dash.
 */
function rowsFor(manifest: Manifest, filename: string): number | undefined {
  const table = filename.replace(/-[0-9a-f]{8}\.parquet$/, '');
  return manifest.rowCounts[table];
}

function megabytes(bytes: number): string {
  return `${(bytes / 1e6).toFixed(1)} MB`;
}
