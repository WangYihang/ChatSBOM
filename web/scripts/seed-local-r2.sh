#!/bin/sh
# Load dist/data into the local R2 simulator, so `npm run dev` has a
# dataset to query.
#
# The Worker reads Parquet from the DATA bucket rather than from static
# assets, because assets cap at 25 MiB per file and R2 serves the ranged
# reads DuckDB-WASM issues. A fresh clone's local bucket is empty, so
# without this the dashboard loads and then reports that it could not
# fetch the dataset manifest.
#
# The bucket seeded is `preview_bucket_name`, not `bucket_name`: a local
# `wrangler dev` binds the preview bucket, so seeding the production name
# writes objects nothing reads. Confirmed against the dev server's own
# bucket listing.
#
# Only .wrangler/state is touched. Nothing is uploaded anywhere.
set -eu

cd "$(dirname "$0")/.."

BUCKET=$(node -e '
// wrangler.jsonc has comments, so strip line comments before parsing
// rather than adding a JSONC dependency for one field. Comment markers
// inside string values would break this; there are none, and the
// assertion below fails loudly if the shape ever changes.
const fs = require("fs");
const text = fs.readFileSync("wrangler.jsonc", "utf8");
const config = JSON.parse(text.replace(/^\s*\/\/.*$/gm, ""));
const bucket = (config.r2_buckets || []).find((b) => b.binding === "DATA");
if (!bucket) throw new Error("no r2_buckets entry bound as DATA in wrangler.jsonc");
process.stdout.write(bucket.preview_bucket_name || bucket.bucket_name);
')

# Parquet filenames are content-addressed, so a re-export leaves the
# previous generation behind. Clear the bucket first: an old file that
# nothing references is harmless locally but wastes the seeding time,
# and a *partially* cleared bucket is how you end up debugging a file
# that no manifest names.
if [ ! -f dist/data/manifest.json ]; then
  echo "dist/data is empty. Generate it first, from the repository root:" >&2
  echo "  uv run chatsbom export parquet --output web/dist/data" >&2
  exit 1
fi

echo "Seeding local bucket $BUCKET:"
for path in dist/data/*; do
  key=${path##*/}
  case $key in
    *.json) type=application/json ;;
    *) type=application/vnd.apache.parquet ;;
  esac
  printf '  %-24s %s\n' "$key" "$(wc -c <"$path" | tr -d ' ') bytes"
  npx wrangler r2 object put "$BUCKET/$key" \
    --file "$path" --content-type "$type" --local >/dev/null 2>&1
done

# The query engine's WebAssembly module lives in the same bucket, for
# the reasons documented in src/duckdb.ts. Without it the dashboard boots
# and then reports that the engine could not be fetched.
WASM=node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm
if [ -f "$WASM" ]; then
  printf '  %-24s %s\n' duckdb-eh.wasm "$(wc -c <"$WASM" | tr -d ' ') bytes"
  npx wrangler r2 object put "$BUCKET/duckdb-eh.wasm" \
    --file "$WASM" --content-type application/wasm --local >/dev/null 2>&1
else
  echo "  duckdb-eh.wasm missing — run npm install" >&2
  exit 1
fi

echo "Done. Now: npm run dev"
