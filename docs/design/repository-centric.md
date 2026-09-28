# Repository-centric ChatSBOM

| | |
|---|---|
| Status | Draft for review. Nothing in this document is implemented yet. |
| Base | `origin/main` at `de60629` (2026-09-28). This includes #52, which landed the 13 local commits (landing zone, manifests in the database, slim ledgers, `chatsbom run`, language-worth measurement), and #49. |
| Motivating issue | #51: dependency coverage gap. Manifests are fetched from the repository root only, depgraph is gated on SBOM success, and manifest sets are keyed by language. |
| Scope | Collection, storage layout, scheduling, transform, rollups, D1 export, web. |
| Out of scope | The OpenAPI commands (`chatsbom openapi *`), except where they read a stage path. The LLM classifier. |

Every line reference below is to `de60629`. Every number was measured on
2026-09-28 against `data/` and `.cache/`, which are symlinks into
`/mnt/hdd-tank/chatsbom/`. All measurements were read-only; the
"Measurements" appendix says how each one was taken.

---

## 1. Summary

1. **One repository list.** The SQLite ledger (`data/ledger.sqlite3`) becomes the single list of repositories. It is seeded from one unfiltered GitHub search snapshot. GitHub's language is a column on it and nothing else: no list, path, loop or CLI option is keyed by language any more.
2. **Repository-keyed paths.** Every stage artefact moves from `<stage>/<lang>/<owner>/<repo>/<ref>/<sha>/…` to `<stage>/<owner>/<repo>/<sha>/…`. The depgraph moves from `<lang>/<owner>/<repo>/sbom.spdx.json` to `<owner>/<repo>/<fetched>-<head12>/sbom.spdx.json`. A journaled `data migrate-layout` command moves the existing files with `rename(2)` on the same filesystem, so nothing is re-fetched. It supports dry-run, verify and rollback.
3. **Manifest discovery reads the tree.** Content is chosen from each repository's stored `tree.txt`, at any depth, from the union of every ecosystem's manifest and lockfile names. The base set is `sbom_service.MANIFEST_NAMES`/`MANIFEST_SUFFIXES`; the per-language extras and the Gradle version-catalog files are added to it. Vendored, fixture, test and example paths are skipped. Each repository is capped at 200 files and 64 MiB. Measured on the stored trees: 284,147 candidate files across 34,620 repositories, 229,721 after the cap (median 2, p99 102). The cap binds on 152 repositories.
4. **Syft scans everything that was discovered.** It is the primary artifact source. `sbom lock` picks a recipe per *directory*, from the manifests present there. Generated lockfiles are merged at the same relative path.
5. **The dependency graph is a supplement that runs on its own.** It is fetched for every tracked repository whether or not the SBOM succeeded. Each fetch is stored permanently, under its own ref and HEAD sha. A 404 is negatively cached for 30 days. The stage turns itself off at `DEPGRAPH_ENDPOINT_CLOSES = 2026-11-13`, and nothing downstream depends on it.
6. **The ledger schedules each stage.** A new `stage_state` table holds one row per repository and stage, with its own lease, backoff, stage version and input key. `chatsbom run --stage X` claims due work for one stage and writes its watermark. With no `--stage`, `chatsbom run` keeps its repository-major walk, but now uses per-stage state. A stage is due when its code version or its input changed, not only when there was a push.
7. **Classification by ecosystem.** Direct or transitive is decided per artifact from its type or purl (canonical names from `core/ecosystems.py`), against that ecosystem's manifests. The repository's language plays no part. A repository with a Java backend under a TypeScript label gets Maven verdicts for its Maven artifacts.
8. **Declared-manifest rows** (`source = 'manifest'`) cover build files that Syft 1.41.2 does not read. Measured: 40 of 40 sampled `build.gradle`-only projects have zero Syft artifacts. Without this source, `halo-dev/halo` can never show `spring-boot-starter-web`. **This needs an owner decision (§12, D1).**
9. **Depgraph rows are stamped with their own ref and sha.** Today they copy the Syft scan's (`db_service.py:486-487`). A single `current_artifacts` view defines the "current scan" per source. It replaces the six inline joins, and the rollups read it instead of all history.
10. **`db index` masters on the ledger.** Every tracked repository gets a `repositories` row, including those with no SBOM and no graph. Records are no longer scoped by the `/<lang>.jsonl` suffix of their landing path (`documents.py:416-424`).
11. **Rollups aggregate by ecosystem** (npm, Maven, PyPI, Go, Composer, …). The one-language-per-repository assumption (`rollups.py:31-36`) is removed, and whole-corpus numbers become `uniqExact` over `current_artifacts`. GitHub language survives as a folded attribute: the top 12 plus `other`.
12. **D1 and web.** The ecosystem becomes the primary filter. The `agg_*` tables are re-keyed and `repositories` gains `ecosystems` and `language_bucket`. The language filter is kept as a repository attribute in the query view.
13. **Release-tag fix.** The code fix is already on main (`deb258e`: tags only, cache version 2). What remains is the data. All 34,728 release caches predate version 2, so every repository's release and commit stages must re-run. Tags are dated with `git` instead of `/commits/{sha}`. Otherwise it would cost about 1.65 M core calls.
14. **Search refresh.** An unfiltered `stars:>=1000` search makes a new dated snapshot (`01-github-search/all-<date>.jsonl`), and the ledger is re-seeded from it. The unfiltered time-slice query, which currently sends `language:None` (`search_service.py:180`), is fixed first.
15. **Order and cost.** The depgraph is the critical path: about 40 k graphs at about 100 per hour is about 17 days, and the endpoint closes in 46 days. So the depgraph change ships first. Everything else is roughly 3 days of wall time and about 80 k core API calls. Worst-case disk growth is about 105 GB against 304 GB free.

---

## 2. What main does today (re-verified on `de60629`)

The earlier read-only map was made before #52. These rows correct or confirm it.

| # | Finding | Where | Status vs. the earlier map |
|---|---|---|---|
| F1 | Stage lists are `<stage>/<lang>.jsonl`, and every `get_*_list_path(language)` takes a language. | `core/config.py:127-168` | Confirmed. |
| F2 | Artefact paths are `<stage>/<lang>/<owner>/<repo>/<ref>/<sha>/`. The depgraph is `09-github-depgraph/<lang>/<owner>/<repo>/sbom.spdx.json`. | `config.py:149-168`, `content_service.py:64-67`, `sbom_service.py:260-269` | Confirmed. |
| F3 | `github content` fetches `LanguageFactory.get_handler(lang).get_sbom_paths()` at the root only. The stored tree is ignored. | `content_service.py:69-82`, `models/language.py:26-200` | Confirmed. |
| F4 | `chatsbom run` exists and is the collector's worker. It claims per stage, then collapses the claims to repositories (`run_service.py:163-188`) and walks the whole chain for each. The stages are RELEASE, COMMIT, TREE, CONTENT, DEPGRAPH, SBOM (`run_service.py:74-81`). | `commands/run.py:176-371`, `services/run_service.py` | New. |
| F5 | Inside `chatsbom run`, **depgraph already runs whether or not there is an SBOM**: it sits in the chain before SBOM (`run.py:292`). The standalone `github depgraph` command still reads the 07-sbom list, so there it is still gated on SBOM success. | `commands/github/depgraph.py:78`, `run.py:109-139` | Partly corrected. |
| F6 | `chatsbom run` skips content for any repository whose language is not one of the 9 enum values (`run.py:263-272`). It never stores a record for a repository with no language (`run.py:318-322`). | `run.py` | New. The polyglot gap persists in the worker. |
| F7 | The ledger lease is **per repository**, not per stage: there is one `claimed_by` column (`ledger.py:172-192`, `514-558`). A stage is due only when `pushed_at_seen` overtakes its watermark (`ledger.py:151-156`), so a change to a stage's *code* never makes it due. | `core/ledger.py` | New. |
| F8 | `db index` reads `raw_documents` by default (`index.py:122-127`). It loops over `Language` (`index.py:150`), and `RawRecords` scopes records by the landing path suffix `/<lang>.jsonl` (`documents.py:412-424`). Its master list is the `repo` records, and those are written only by `chatsbom run` / `sbom generate` through `RecordStore` (`run.py:318-322`). A repository that never finished a chain is never indexed. | `commands/db/index.py`, `core/documents.py` | Corrected. It no longer masters on the 07 file; it masters on repo records scoped by language. |
| F9 | Depgraph artifact rows are stamped with the **Syft scan's** `sbom_ref`/`sbom_commit_sha`, taken from the repository row. The graph actually describes the default branch at fetch time. | `services/db_service.py:483-492` | New. |
| F10 | "Current scan" means joining on `a.sbom_commit_sha = r.sbom_commit_sha`. The join is inlined 6 times in `export/queries.py` (lines 48, 71, 133, 142, 177, 190) and once in `core/repository.py:425`. The rollups do **not** join at all: they aggregate every stored scan (`rollups.py:77-102`). | | New. |
| F11 | `forget_scans` deletes by `(repository_id, sbom_commit_sha)` across **all sources**. | `core/repository.py:358-398` | New. It must become source-aware once depgraph rows carry their own sha. |
| F12 | Direct/transitive classification is `relationships_from(read, Language(repo.language))`. It is keyed by the repository's language, and returns `None` for any other language. | `db_service.py:340-351`, `core/manifest.py:523-570`, `683-745` | Confirmed. |
| F13 | `sbom lock` recipes are keyed by `Language` (`core/sandbox.py:155-179`, `226-242`). They resolve at the project root only. Generated lockfiles are merged into the scan tree's **root** (`sbom_service.py:182-216`, `302-304`). | | Confirmed. The root-only merge is new. |
| F14 | The rollups assume one language per repository: "every repository has exactly one language … summing a per-language distinct count … double-counts nothing" (`rollups.py:31-36`). `mv_packages` sums `mv_package_language` (`rollups.py:199-208`), and so does `mv_top_packages` (`382-410`). | `core/rollups.py` | Confirmed. |
| F15 | D1: `agg_relationship_split`, `agg_language_coverage`, `agg_top_packages` and `agg_source_comparison` are keyed by language. The overall `agg_top_packages` row sums repository counts across languages. | `export/d1.py:308-395`, `812-966` (lines 905-910) | Confirmed. |
| F16 | Prune assumes `SCAN_DEPTH = 5` and groups by `(language, owner, repo)`. | `core/prune.py:30`, `94-95` | Confirmed. |
| F17 | The Syft cache key parses `<lang>/<owner>/<repo>/<ref>` from `rel_path`. | `sbom_service.py:333-345`, `config.py:101-119` | Confirmed. Also measured: 30,691 legacy **unversioned** entries in `.cache/syft/<owner>/…`, which the current key (`syft/<version>/…`) never reads; 1,317 entries under `1.41.2/`. |
| F18 | The release-tag fix **is on main**, as `deb258e` "fix(release): branches were releases…" (PR #49). It adds `GitService.get_repo_tags` (`git_service.py:121`, `refs/tags/*` only), `RELEASE_CACHE_VERSION = 2` (`models/github_release.py`), and the refetch of stale caches (`release_service.py:53-75`). `claude/polyglot-repos@857f410` is superseded and does not need cherry-picking. | | Corrected. |
| F19 | All **34,728** stored release caches are pre-version-2: 99.5% of them are bare API lists and the rest are `version: 1` objects. Every repository's `latest_stable_release` and `download_target` were computed with branches counted as tags. With `.requests-cache` expired (7-day TTL), re-running the release stage as written spends one `/commits/{sha}` call per tag without a release. That is a mean of 47.4 per repository, about **1.65 M** calls. | `release_service.py:122-147` | New. |
| F20 | The unfiltered search's time-slicing sends `language:None stars:N created:…` when `lang` is `None`. | `services/search_service.py:180` | New bug. |
| F21 | `raw_documents.path` stores the on-disk path, and readers derive meaning from it in two places: `RawManifests._inside` strips `CONTENT_PREFIX_DEPTH = 5` (`documents.py:59`, `262-278`, and again in `db/raw.py:104`), and `RawRecords` filters on the suffix. `db raw` skips unchanged files by `(kind, repository_id, path)` (`raw.py:341-390`). | | New. This is why the migration must rewrite `path`. |
| F22 | Syft 1.41.2 produces **no artifacts** from `build.gradle`/`.kts`: 40 of 40 sampled Gradle-only Java projects in `07-sbom` have an empty `artifacts` array. It reads `pom.xml` and `gradle.lockfile`. | measured | New. It drives D1. |
| F23 | Ledger snapshot (`data/ledger.sqlite3`, 2026-09-20): 31,959 rows. `language` is the *list* a repository was tracked from (`queue/track.py:32-40`), not GitHub's language. Watermarks: sbom 28,069, content 28,069, depgraph 24,946, repo 4,008. | | New. |
| F24 | Search: `all.jsonl` (2026-03-09) has 60,017 repositories; the eight language lists together hold 34,621. On the 720-repository Spring cross-check, 672 are in `all.jsonl` and 605 are in the language lists. Only 140 of the 720 have Spring evidence at the root. | `notes/selection-pool/spring.csv` in the paper repository | New. |

---

## 3. Goals and non-goals

**Goals**

- Every repository with at least 1,000 stars in the current snapshot is collected, whatever its GitHub language.
- Every manifest ecosystem present anywhere in a repository reaches Syft, the classifier and the rollups.
- The dependency graph is collected for every repository before the endpoint closes, and is kept permanently afterwards.
- The collector keeps up with the corpus continuously, one stage at a time, and each stage is budgeted separately.
- Existing data is moved, not re-fetched.

**Non-goals**

- New ecosystems beyond what Syft and the eight existing manifest parsers read. NuGet, Swift, pub and conan are discovered and scanned by Syft, but get no direct/transitive parser (their verdicts stay `unknown`, which is honest).
- Resolving Gradle or Maven builds in a sandbox. That is still ruled out by `sandbox.py:196-224`.
- Replacing ClickHouse, D1 or the Worker.

---

## 4. Target architecture

### 4.1 The repository list and the ledger (decisions 1, 6)

The ledger's `repository_state` table **is** the repository list. Nothing else enumerates repositories:

- The per-stage JSONL ledgers stop being inputs. They remain only as optional append-only audit logs, `<stage>/index.jsonl`, one per stage and not per language.
- `raw_documents` holds the documents.

The following columns are added to `repository_state`. They are additive, applied through the existing `_ADDED_COLUMNS` mechanism (`ledger.py:197-199`).

| Column | Meaning |
|---|---|
| `github_language TEXT` | GitHub's `language`, verbatim (for example `TypeScript`, `C++`, or empty). This is an attribute only. |
| `snapshot TEXT` | The search snapshot that last listed the repository (for example `all-2026-10-01`). Empty means it is no longer in the current snapshot. |
| `stars INTEGER`, `default_branch TEXT` | From the snapshot. Used for ordering and for the depgraph stamp. |

The existing `language` column is kept, so old readers still open the file. It is no longer written after the migration. `track()` writes `github_language` instead.

The new table `stage_state` replaces both `stage_watermarks` (JSON) and the repository-level lease:

```sql
CREATE TABLE stage_state (
    repository_id    INTEGER NOT NULL,
    stage            TEXT    NOT NULL,       -- Stage value
    done_at          TEXT,                   -- last success
    stage_version    INTEGER NOT NULL DEFAULT 0,
    input_key        TEXT    NOT NULL DEFAULT '',  -- what it consumed
    output_key       TEXT    NOT NULL DEFAULT '',  -- what it produced
    outcome          TEXT    NOT NULL DEFAULT '',  -- ok|empty|absent|failed|skipped
    http_status      INTEGER,
    failure_count    INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TEXT,
    last_error       TEXT    NOT NULL DEFAULT '',
    claimed_by       TEXT    NOT NULL DEFAULT '',
    claim_expires_at TEXT,
    PRIMARY KEY (repository_id, stage)
);
CREATE INDEX idx_stage_due ON stage_state (stage, next_attempt_at);
```

**When a stage is due.** A stage S with upstream stage U is due when every one of the following holds:

- `next_attempt_at <= now`, and there is no live lease on (repository, S).
- At least one of these is true:
  - there is no row;
  - `stage_version < STAGE_VERSION[S]`;
  - `input_key != stage_state[U].output_key`;
  - for REPO only, the recheck interval has elapsed. This is today's clock rule, from `ledger.py:144-149`.

This is what the push-only rule at `ledger.py:151-156` cannot express. Bumping `STAGE_VERSION[CONTENT]` from 1 to 2 makes all 34.6 k repositories due for discovery, with no push and no manual reset.

| Stage | Upstream | `input_key` | `output_key` | Version after this change |
|---|---|---|---|---|
| REPO | (clock) | — | `pushed_at` | 1 |
| RELEASE | REPO | `pushed_at` | tag chosen, or `''` | **2** (tags only, see F18) |
| COMMIT | RELEASE | tag, or default-branch name | commit sha | 2 |
| TREE | COMMIT | commit sha | commit sha | 1 |
| CONTENT | TREE | commit sha | sha256 of the discovery list | **2** |
| LOCK | CONTENT | discovery sha | sha256 of the generated lockfiles | 2 |
| SBOM | CONTENT (+LOCK) | `content_fingerprint` | sha256 of the Syft document | **2** |
| DEPGRAPH | (clock, see §4.8) | — | document sha256 | **2** |
| INDEX | SBOM, DEPGRAPH, REPO | concatenated output keys | — | 2 |

`Ledger.claim(stage, …)` leases `stage_state` rows, not `repository_state` rows. A depgraph worker and a content worker can then hold the same repository at the same time. Their failures and backoff stay separate: a depgraph 500 no longer backs off Syft, which `record_failure` at `ledger.py:443-465` does today because the backoff lives on the repository.

**CLI**

- `chatsbom run --stage depgraph --limit N --quota Q` claims and runs one stage. The collector can then run a depgraph worker at the pace of its own bucket beside a core-API worker.
- `chatsbom run` without `--stage` keeps the repository-major walk from `run_service.py:190-267`. It records into `stage_state` and walks only the stages that are due, in order. A stage that is not due costs nothing, because its outputs are pure functions of `(owner, repo, sha)` (§4.3), so no `carried` hand-off is needed.
- `--language` is removed from `run`, `queue sync`, `queue track`, `db index`, `db raw` and every `github *` / `sbom *` command. For pilots and trials there is `--repos-file PATH` (one `owner/repo` per line) instead.
- `queue track --snapshot data/01-github-search/all-<date>.jsonl` seeds the ledger from a search snapshot. It replaces reading `02-github-repo/<lang>.jsonl` (`track.py:32-40`).
- `queue status` reports due, failing and leased counts per stage from `stage_state`.

### 4.2 The stage graph

```
search snapshot ─► queue track ─► REPO (queue sync, 304s free)
                                   │
                  ┌────────────────┴──────────────────────┐
                  ▼                                       ▼
              RELEASE ─► COMMIT ─► TREE ─► CONTENT ─► LOCK ─► SBOM ─┐
                                                                    ├─► db raw ─► INDEX
              DEPGRAPH (independent; clock + negative cache) ───────┘
```

DEPGRAPH has no upstream in the pipeline. It needs only `owner/repo`, plus the default-branch HEAD sha for its stamp, which `git ls-remote` gives for free (§4.8). LOCK and SBOM do not wait on DEPGRAPH, and INDEX takes whatever of the two exists.

### 4.3 Storage layout (decision 5)

The path identity is the owner and repository name as GitHub spells them, plus the commit sha. The ref disappears from paths: it is metadata of the download target (`models/download_target.py`), and two refs at one sha are one scan. Measured collisions today:

- No `owner/repo` appears under two language directories, in 05, 06, 07 or 09.
- Three repositories have the same sha under two refs in 05 and 06, all `HEAD` and `main` (left over from the release bug). They collapse into one directory (§7.1).

| Artefact | Today | After |
|---|---|---|
| Search | `01-github-search/<lang>.jsonl`, `all.jsonl` | `01-github-search/all-<YYYY-MM-DD>.jsonl`. Immutable once written. `all.jsonl` is kept as `all-2026-03-09.jsonl`. |
| Repo metadata | `02-github-repo/<lang>.jsonl` | `raw_documents` kind `repo-metadata`, plus the optional `02-github-repo/index.jsonl` audit log. |
| Tree | `05-github-tree/<lang>/<o>/<r>/<ref>/<sha>/tree.txt` | `05-github-tree/<o>/<r>/<sha>/tree.txt` and `…/<sha>/manifests.json` (the discovery list, §4.4). |
| Content | `06-github-content/<lang>/<o>/<r>/<ref>/<sha>/<path>` | `06-github-content/<o>/<r>/<sha>/<path-in-repo>` |
| SBOM | `07-sbom/<lang>/<o>/<r>/<ref>/<sha>/sbom.json` | `07-sbom/<o>/<r>/<sha>/sbom.json` |
| Depgraph | `09-github-depgraph/<lang>/<o>/<r>/sbom.spdx.json` (overwritten on every fetch) | `09-github-depgraph/<o>/<r>/<fetched YYYYMMDD>-<head12>/sbom.spdx.json` plus `meta.json`. Never overwritten; a document identical to the previous one is not stored again. Legacy documents go to `…/<o>/<r>/legacy/`. |
| Generated lock | `10-generated-lock/<lang>/<o>/<r>/<sha>/<lockfile>` | `10-generated-lock/<o>/<r>/<sha>/<dir-in-repo>/<lockfile>` |
| Syft cache | `.cache/syft/<ver>/<o>/<r>/<ref>/<hash>.json` | `.cache/syft/<ver>/<o>/<r>/<hash>.json`. The content hash already identifies the input. |
| Tree cache | `.cache/git-tree/<o>/<r>/<ref>/<sha>/tree.txt` | `.cache/git-tree/<o>/<r>/<sha>/tree.txt`. It duplicates the 05 file and could be dropped later; not in this change. |

`PathConfig` (`core/config.py:11-168`) loses every `language` parameter. The new signatures are:

```python
def tree_file(owner, repo, sha) -> Path
def discovery_file(owner, repo, sha) -> Path          # manifests.json
def content_root(owner, repo, sha) -> Path
def sbom_file(owner, repo, sha) -> Path
def depgraph_dir(owner, repo) -> Path                 # all fetches
def depgraph_file(owner, repo, fetched: date, head_sha) -> Path
def generated_lock_dir(owner, repo, sha) -> Path
def get_sbom_cache_path(owner, repo, content_hash, syft_version) -> Path
def search_snapshot(date) -> Path
```

Renames and transfers: `queue sync` already sees the repository resource. When `full_name` changes for a known `repository_id`, it moves `<stage>/<old_o>/<old_r>` to the new name for every stage root, as one journaled rename per stage, and updates the ledger row. The same code serves the migration.

### 4.4 Manifest discovery (decision 2)

A new module, `chatsbom/core/discovery.py`, holds one pure function:

```python
def discover(tree_paths: Iterable[str], *, max_files=200,
             max_bytes=64 * 2**20) -> Discovery
```

`Discovery` has two fields:

- `selected`: a list of `(path, ecosystem)`.
- `skipped`: a list of `(path, reason)`. `reason` is `excluded-dir`, `over-file-cap` or `over-byte-cap`; the byte cap is checked at download time.

**Names.** The union of:

- `sbom_service.MANIFEST_NAMES` and `MANIFEST_SUFFIXES` (`sbom_service.py:58-75`);
- every `BaseLanguage.get_sbom_paths()` entry not already in that union: `environment.yml`, `requirements-dev.txt`, `requirements_dev.txt`, `dev-requirements.txt`;
- the Gradle build-logic files the Spring cross-check depends on: `settings.gradle`, `settings.gradle.kts`, `gradle/libs.versions.toml`, `*.versions.toml`, `gradle.properties`. Eleven of the 720 Spring repositories are `catalog_only`.

`MANIFEST_NAMES` then becomes the single registry. It moves to `core/discovery.py`, with `sbom_service` importing it, and `get_sbom_paths()` is deleted. `vendor/modules.txt` is kept as a path rule, overriding the `vendor/` exclusion, because Go writes it there on purpose.

**Excluded directories.** A match on any path segment, case-sensitive:

- `node_modules`, `vendor`, `third_party`, `third-party`, `.venv`, `venv`, `site-packages`, `bower_components`, `Pods`;
- `test`, `tests`, `__tests__`, `testdata`, `test-data`, `fixtures`, `__fixtures__`;
- `example`, `examples`, `sample`, `samples`, `demo`, `demos`;
- `benchmark`, `benchmarks`, `e2e`;
- `.yarn`, `dist`, `target`, `build`.

This is `manifest.VENDOR_DIRS` (`manifest.py:54-57`) extended. Both modules import the one list. `docs/` is **not** excluded: a documentation site is still a dependency of the project. The choice of `examples/` and `samples/` is open decision D5.

**Order and caps.** Selected paths are sorted by depth, then lockfile before manifest, then name. The first 200 are kept. Measured on the 34,620 stored trees with these rules:

| | Repositories |
|---|---|
| any discoverable manifest | 31,104 (89.8%) |
| none | 3,516 |
| discoverable only below the root (the #51 gap) | 2,300 — go 212, java 370, javascript 468, php 68, python 784, ruby 27, rust 102, typescript 269 |
| more than one ecosystem | 4,846 (14.0%) |
| over the 200-file cap | 152 (max 5,376) |

Files: 284,147 before the cap and 229,721 after. Per repository: median 2, p90 12, p99 102. The earlier map's "~2,800" used a shorter exclusion list; with the tests/examples exclusions above it is 2,300.

`discover` runs **after TREE** and writes `05-github-tree/<o>/<r>/<sha>/manifests.json`. That file is also landed as `raw_documents` kind `content-index`, so every "why was this manifest not scanned" question can be answered from the database.

### 4.5 Content fetch

`ContentService.process_repo` (`content_service.py:47-137`) takes `(repository, discovery)` instead of `(repository, language)`. It fetches `raw.githubusercontent.com/<o>/<r>/<sha>/<path>` for each selected path into `content_root(o, r, sha)/<path>`. Three things are kept: atomic writes (`content_service.py:104-108`), skip-if-present, and 404 as "not there". Two things are new:

- a per-file byte cap of 16 MiB, stopping the download early via `stream=True`;
- a per-repository byte cap of 64 MiB, after which the remaining files are recorded as `over-byte-cap`.

The output key is the sha256 of the sorted `(path, size)` list actually written.

*Alternative, kept for the pilot.* The tree stage already makes a blobless clone (`git_service.py:182-258`). `git checkout <sha> -- <paths…>` in that clone fetches every selected blob in one batched request and spends no API quota. Raw GETs are simpler and already hardened, so they are the default. If the pilot sees `raw.githubusercontent.com` throttling (429 or secondary limits), content switches to the batched git fetch behind the same `ContentService` interface.

### 4.6 SBOM generation and lockfiles

**`SbomService.process_repo`** (`sbom_service.py:232-318`):

- It takes `(owner, repo, sha)`. `rel_path` parsing (`sbom_service.py:333-341`) goes away.
- Output goes to `sbom_file(o, r, sha)`. The cache key is `(owner, repo, content_fingerprint, syft_version)`.
- Syft still scans `dir:` over the whole content root. It now contains every discovered manifest, so a repository labelled TypeScript with a Maven backend yields Maven artifacts.
- The 600 s timeout (`sbom_service.py:27`) stays. A repository that times out is recorded as `outcome = failed, last_error = 'syft timeout'` and retried with backoff. Its depgraph and manifest rows still reach the index.

**`sbom lock` chooses its recipe from the manifests present.** `LOCK_RECIPES` is re-keyed by ecosystem: `{'composer': …, 'gem': …}` (`sandbox.py:155-179`). For every directory in the discovery list that has the recipe's manifest (`composer.json` / `Gemfile`) and none of its `produces` names, the recipe runs on that directory. It writes to `generated_lock_dir(o, r, sha)/<dir>/`, with at most 10 directories per repository. `_lockfiles_to_merge` (`sbom_service.py:182-216`) then copies each generated file to the **same relative directory** in the merged scan tree, not to the root (`sbom_service.py:302-304`). `DISABLED_RECIPES` becomes `{'maven': …, 'pypi': …}`, with the same reasons as before.

### 4.7 Declared-manifest source (needs decision D1)

Syft reads nothing from Gradle build files (F22), and GitHub's graph is partial for Gradle: for halo it has 105 packages and no Spring starter, per #51. So a Gradle-only repository cannot show `spring-boot-starter-web` from either source. The project already parses those files for classification (`manifest._parse_gradle`, `manifest.py:457-487`). The proposal is to emit what it parses as artifact rows:

- `source = 'manifest'`, `type = 'maven'`, `purl = pkg:maven/<group>/<artifact>`.
- `version_kind` is `constraint` or `unversioned`, via `provenance.classify_version`.
- `relationship = 'direct'`.

`_parse_gradle` and `_parse_pom` must keep `group:artifact`, not only the artifact id. Version catalogs (`libs.x.y`) are resolved against `gradle/libs.versions.toml` when it was downloaded; a reference that cannot be resolved stays `incomplete`, as today.

The rows are emitted **only for build files Syft does not read**: `build.gradle(.kts)`, `settings.gradle(.kts)` and `*.versions.toml`. Syft already reports `pom.xml` declarations (`java-pom-cataloger`), so emitting those twice would double-count.

### 4.8 Dependency graph (decision 3)

**Due.** DEPGRAPH is due for every tracked repository in the current snapshot when either:

- there is no `stage_state` row; or
- `outcome = ok` and `done_at` is older than `DEPGRAPH_REFRESH = 30 days`.

In both cases `next_attempt_at <= now` must also hold. The stage is independent of SBOM, of CONTENT and of language. `github depgraph` (`depgraph.py:76-97`) becomes a thin alias of `chatsbom run --stage depgraph`, so the SBOM-list gate at `depgraph.py:78` goes away.

**Outcomes**, extending the four that `DependencyGraphService.fetch` already distinguishes (`dependency_graph_service.py:187-246`):

| GitHub answer | `outcome` | Next attempt |
|---|---|---|
| 200 with an SPDX object | `ok` | +30 days |
| 404 (no graph, or the graph is disabled) | `absent` — the negative cache | +30 days, then 60, capped at 90 |
| 500 "Request timed out" (large repositories, for example spring-boot) | `failed`, with `http_status = 500` | Backoff: 15 min doubling (`ledger.backoff_for`). After 5 consecutive 500s, `outcome = too_large` and +30 days. |
| 403 or 429 (refused) | nothing recorded | The worker stops, as `run.py:120-127` does today. |
| Any other failure | `failed` | Backoff |

**Stored permanently.**

- The document goes to `depgraph_file(o, r, fetched, head_sha)`.
- A `meta.json` beside it holds `{ref: default_branch, commit_sha: head_sha, fetched_at, http_status, sha256}`. `head_sha` is `refs/heads/<default_branch>` from a `git ls-remote` made immediately before the fetch. That call costs no quota, and `get_repo_refs` already makes it (`git_service.py:27`).
- Documents are never overwritten or pruned. `data prune` never touches `09-github-depgraph`.
- `db raw` lands each fetch as its own `raw_documents` row. The row carries its `ref` and `commit_sha` in two new, additive columns (§4.11).

**Closing.** `DEPGRAPH_ENDPOINT_CLOSES = date(2026, 11, 13)` is set in `dependency_graph_service.py`, next to `ENDPOINT` (line 178). From that date DEPGRAPH is never due. `queue status` reports it as `closed`, and `db index` keeps using the last stored document for each repository. No stage lists DEPGRAPH as an upstream, so nothing else changes. If the asynchronous successor endpoint (the note at `dependency_graph_service.py:175-177`) proves usable, it can be added later as a new `STAGE_VERSION` of DEPGRAPH. This design does not depend on it.

**Budget.** About 100 requests per hour, measured and noted at `run.py:201-205`. The worker is a separate process, `run --stage depgraph --quota 90` every hour, so the rest of collection is never paced by this bucket. Its order is:

1. repositories never asked;
2. `ok` documents older than 30 days;
3. `absent` rows whose negative cache has expired.

### 4.9 Release and tag follow-through (decision 4)

The code fix is in (F18). What this redesign adds:

1. **Date tags with git, not the API.**
   - After `ls-remote`, run `git init --bare` in a temporary directory, then `git fetch --depth=1 --filter=tree:0 <url> 'refs/tags/*:refs/tags/*'`.
   - Then run `git for-each-ref --format='%(refname:strip=2) %(creatordate:iso-strict) %(*committerdate:iso-strict)' refs/tags`.
   - This yields commit and tagger dates for every tag. It fetches commit and tag objects only, with no trees and no blobs, and spends zero REST calls.
   - It replaces the per-tag `get_commit_date` loop (`release_service.py:122-147`). `get_commit_date` stays only as a fallback when the fetch fails, capped at 20 tags per repository and ordered by version-sort descending.
   - Measured: the mean over the corpus falls from 47.4 calls per repository to **0**. The capped fallback would cost 6.1 per repository.
2. **Re-run everything once.** `STAGE_VERSION[RELEASE] = 2` makes every repository due. COMMIT follows through its `input_key`. When the resolved sha differs from the stored one — a repository whose "latest release" was a branch — TREE, CONTENT and SBOM become due through their input keys. When the sha is unchanged, they are not.
3. **Pilot checks.** mathesar-foundation/mathesar and WebGoat/WebGoat must resolve to a real tag or to the default branch, never to `github-repo-stats` or `feat/password-storage-lesson` (§10).

### 4.10 Direct/transitive classification by ecosystem

`manifest.py` changes:

- `_PARSERS` is re-keyed by canonical ecosystem instead of `Language` (`manifest.py:523-562`): `npm`, `gem`, `go`, `cargo`, `pypi`, `composer`, `maven` (pom + gradle).
- `parser_for(ecosystem)` replaces `parser_for(language)`.
- `relationships_from(manifests)` returns `dict[ecosystem, DirectDependencies]`. It groups manifests by the parser whose `matches(name)` accepts them; each filename belongs to exactly one ecosystem.
- `resolve_relationships(content_dir)` loses its `language` argument. `_find_manifests`' `max_depth = 3` is dropped, since discovery already bounded the set.

`db_service` changes:

- `_direct_dependencies(repo, manifests)` (`db_service.py:340-351`) no longer reads `repo.language`.
- `parse_artifacts` classifies each artifact with `by_ecosystem.get(canonical(art.type))`, falling back to the purl type when `type` is empty. With no entry, the verdict is `unknown`.
- A Syft `java-archive`/`maven` artifact in a TypeScript-labelled repository is judged against its `pom.xml`/`build.gradle`, not against `package.json`.

**Phase 2 (optional, not required for acceptance).** Scope "direct" to the nearest manifest directory, using Syft's `locations[].path`. The union across directories matches today's behaviour (`relationships_from` merges all directories), so this is left out of the first cut.

`repositories.manifest_sources` stays. It gains `ecosystems Array(LowCardinality(String))`: the canonical ecosystems present in the current scan's artifacts or discovery list.

### 4.11 Landing and indexing

**`raw_documents`** gains two additive columns, `ref String DEFAULT ''` and `commit_sha String DEFAULT ''`:

- SYFT rows get the download target.
- DEPGRAPH rows get `meta.json`; legacy rows stay `''`.
- CONTENT rows get the scan sha.

A new kind, `content-index`, holds the discovery lists. `path` is stored **relative to `base_data_dir`**, not as the absolute path it is today. `RawManifests._inside` (`documents.py:262-278`) then strips a fixed 4-part prefix: `06-github-content/<o>/<r>/<sha>`. `CONTENT_PREFIX_DEPTH` is defined once, in `documents.py`, and `db/raw.py:104` imports it.

**`db raw`** walks the ledger, not the `*.jsonl` listings (`raw.py:154-157`, `213-216`, `278-280`). For each tracked repository it computes the stage paths for its current sha. For DEPGRAPH it lists `depgraph_dir(o, r)`, which covers every stored fetch. `RECORD_SOURCES` (`raw.py:80-82`) reads `raw_documents` `repo-metadata` as today, and no longer depends on per-language files.

**`db index`**

- **Master list: the ledger's tracked repositories in the current snapshot.** For each one, the transform takes:
  - the newest `repo` record (`RawRecords` with no language scope, `documents.py:368-437`), else a minimal record built from the ledger row and `repo-metadata`;
  - the SBOM for the record's `download_target.commit_sha`;
  - the newest DEPGRAPH document and its stamp;
  - the manifests.
- A repository with none of these still gets a `repositories` row. `with_sbom` and `with_depgraph` then become honest coverage numerators, measured against every repository, not only the ones that finished.
- It is incremental by default. It indexes repositories whose INDEX `input_key` (the concatenated SBOM, DEPGRAPH and REPO output keys) changed. `--rebuild` still rebuilds everything.
- `--language` is removed (`index.py:32`, `75-95`, `150-161`). `--repos-file` is the narrowing option and is refused with `--rebuild` for the same reason as today.
- `RecordStore.remember(record, ledger)` (`documents.py:519-561`) stores `path = 'ledger'` rather than a per-language file. RawRecords no longer interprets `path`.

**Stamping and the current-scan join.**

- `parse_dependency_graph` (`db_service.py:470-492`) stamps rows with the document's own `ref` and `commit_sha`. For a legacy document with no known head, it uses `sbom_ref = default_branch, sbom_commit_sha = ''`.
- `repositories` gains `depgraph_ref`, `depgraph_commit_sha` and `depgraph_observed_at`.
- `forget_scans` takes `(repository_id, source, sbom_commit_sha)` (`repository.py:358-398`). Otherwise a Syft re-ingest at sha X deletes a depgraph observation that happens to be at the same X.
- One view in `core/schema.py` defines "current":

  ```sql
  CREATE VIEW current_artifacts AS
  SELECT a.* FROM artifacts a
  INNER JOIN (SELECT id, sbom_commit_sha, depgraph_commit_sha
              FROM repositories FINAL) r ON a.repository_id = r.id
  WHERE a.sbom_commit_sha = if(a.source = 'github-depgraph',
                               r.depgraph_commit_sha, r.sbom_commit_sha)
  ```

  `export/queries.py` (lines 48, 71, 133, 142, 177, 190) and `core/repository.py:419-425` read the view instead of repeating the join. `source = 'manifest'` rows share the Syft scan's sha. When both sources report a package, it counts once per repository, because the rollups count `uniqExact(repository_id)` by name.

### 4.12 Rollups by ecosystem (decision 7)

In `core/rollups.py` every rollup reads `current_artifacts`, not `artifacts`. Today they aggregate every stored scan (F10).

| Today | After |
|---|---|
| `mv_package_language` (name, language) | `mv_package_ecosystem`: `ORDER BY (ecosystem, name)`, where `ecosystem = canonical_sql('type')`. The `records`, `direct_*`, `syft_records` and `depgraph_records` columns stay, and `manifest_records` is added. |
| `mv_language_totals` | `mv_ecosystem_totals` (ecosystem) |
| `mv_packages` = sum over languages | Computed straight from `current_artifacts`: `uniqExact(repository_id)` by name. One repository can have npm and Maven, so summing across ecosystems would double-count. |
| `mv_totals.classified` reads language totals (`rollups.py:223`) | Reads `mv_ecosystem_totals` for the record counts, and `mv_repository_deps` for the repository counts. |
| `mv_language_coverage` | Kept as `mv_language_coverage`, keyed by `language_bucket`: `lower(github_language)` when it is among the top 12 by repository count at refresh time, else `other`, with empty mapped to `none`. The top-12 list is written to a small `language_buckets` table at refresh time, so the export and the web client fold identically. |
| — | New `mv_ecosystem_coverage`: `(ecosystem, repositories, with_syft, with_depgraph, with_manifest)`. A repository counts under every ecosystem it has. |
| `mv_top_packages` (language, direct_only, rank) | `(ecosystem, direct_only, rank)`. `''` still means the whole corpus, now from `mv_packages` without summation. |

The docstring claim at `rollups.py:31-36` is removed and replaced by the invariant that now holds: whole-corpus distinct counts are never sums of group counts.

`scripts/verify_rollups.py` is updated to match. Its per-language ground truths (lines 66-76, 105-115, 180-187) and the one-language assertion (120-134) are replaced with per-ecosystem truths plus a `uniqExact` whole-corpus check. `scripts/benchmark_queries.py` moves to the ecosystem rollups (`ecosystem='composer'` replaces `'php'`), with the same budget: 50 ms total for 22 queries.

### 4.13 D1 export and web

**`export/`**

| Area | Change |
|---|---|
| `queries.py:25`, `50` | `REPOSITORIES_QUERY` exports `github_language`, `language_bucket` and `ecosystems` (as a JSON array in D1). |
| `schema.py:122-125` | Documents the columns. |
| `d1.py` | `agg_relationship_split`, `agg_top_packages` and `agg_source_comparison` are re-keyed by `ecosystem`. `agg_language_coverage` becomes `agg_language_coverage` on `language_bucket` plus a new `agg_ecosystem_coverage`. The overall top-packages row is a `count(DISTINCT)` rather than a sum (`d1.py:905-910`). `KINDS.type` becomes canonical, not raw (`d1.py:160`). |
| `d1.py:450`, `454-455` | Indexes move from `language` to `ecosystem`. |

**Web (`web/src`)**

- `d1/api.ts`: the `language` params at 117-118, 149 and 155-156 become `ecosystem`, and `languageCoverage` gains `ecosystemCoverage`.
- `backend.ts:121-135` and `d1/client.ts:84-133` change their interfaces to match.
- `d1/queries.ts` and `clickhouse/queries.ts` change the corresponding queries: `dependentFilters` keeps a language filter over `language_bucket`, and every aggregate takes `ecosystem`.
- `Overview.tsx:32-44`, `170-176`: the select switches from language to ecosystem.
- `QueryView.tsx:128-129`, `339-345`: keeps both filters, with language options from `language_buckets`.
- `charts/SourceShares.tsx`: gains a `manifest` series.
- `i18n/strings.tsx:93-94`, `162`, `263-294`: new strings.
- `tools.ts:100-102`, `177`, `216-220`: the agent tools get an `ecosystem` parameter.
- `ecosystems.ts` needs no change beyond staying in sync; `tests/ecosystems_test.py` enforces that.

Backwards compatibility: for one release the Worker accepts `language` and maps it to a repository filter, so cached pages keep working.

### 4.14 Search refresh (decision 8)

1. Fix `search_service.py:180` so it builds the filter the way line 51 does (`lang_filter`), which is empty when unfiltered.
2. Run `github search` with no language and `--min-stars 1000`, written to a new `all-<date>.jsonl`. It must be a new file: the resume short-circuit at `search_service.py:38` ends a run at once on a file whose lowest star count already reaches the minimum, and appending to `all.jsonl` would do exactly that.
3. `queue track --snapshot` sets these ledger columns from the snapshot: `snapshot`, `stars`, `github_language`, `default_branch`. It also sets `pushed_at_seen`, so REPO is not needed for new repositories.
4. Repositories in the ledger but not in the new snapshot get `snapshot = ''`. Whether they stay in the rollups is open decision D2.

Stars in `repositories` come from the snapshot or from the fresher `repo-metadata` overlay (`documents.py:440-472`), whichever is newer.

---

## 5. Change list by file

Grouped by the PR that carries it (§8.1). The references are to `de60629`.

| File:line | Change | PR |
|---|---|---|
| `core/config.py:127-168` | Remove every `language` parameter. Add the repository-keyed methods (§4.3) and `search_snapshot(date)`. | A, B |
| `core/config.py:101-119` | Remove `ref` from `get_sbom_cache_path`. | B |
| `core/ledger.py:44-65` | Keep `Stage`. Add `STAGE_VERSION` and an `UPSTREAM` map. | B |
| `core/ledger.py:172-199` | Add the `stage_state` DDL and the new columns (§4.1). Migrate `stage_watermarks` JSON into `stage_state` on open. | B |
| `core/ledger.py:125-156`, `475-558` | Rewrite `needs`, `due` and `claim` per stage. Remove the `language` filter. Add `repos: set[int]` for `--repos-file`. | B |
| `core/ledger.py:340-358` | `track(…, github_language, snapshot, stars, default_branch, pushed_at)` | B |
| `services/run_service.py:74-81`, `163-267` | Per-stage claims. Walk only due stages. Drop `carried` hand-offs in favour of pure path functions. Remove `content_path` (`315-330`). | B |
| `commands/run.py:60-67`, `109-139`, `239-322` | Remove `language_of`. Repository-keyed paths. Add the `--stage` and `--repos-file` options. Run content for every repository (removes the enum gate at `263-272`). Call `remember` with no language (`318-322`). | B, C |
| `commands/run.py:70-173` | `DependencyGraphStage`: negative cache, stamp, permanent path, closing date. | A |
| `services/dependency_graph_service.py:172-246` | `DEPGRAPH_ENDPOINT_CLOSES`. Return the head sha from `ls-remote`. Classify 500 time-outs. | A |
| `commands/github/depgraph.py:76-165`, `214-265` | Becomes an alias of `run --stage depgraph`. Drop the SBOM-list input and the per-language index. | A |
| `commands/queue/track.py:17-49` | `--snapshot PATH`. No language loop. | B |
| `commands/queue/sync.py:31`, `85`; `services/sync_service.py:105-126` | Remove `language`. Detect renames and move the directories. | B |
| `commands/queue/backfill.py:52-56`, `88-89` | Backfill `stage_state` from the new layout. Keep a legacy mode for pre-migration ledgers. | B |
| `core/discovery.py` (new) | `discover()`, the name registry, exclusions and caps. | C |
| `services/content_service.py:47-137` | `process_repo(repository, discovery)`. Byte caps. Output key. | C |
| `commands/github/content.py:196-270` | No language loop. Claims CONTENT through the ledger. | C |
| `commands/github/tree.py:85-160` | No language loop. Writes `manifests.json`. | C |
| `models/language.py:26-200` | Delete `get_sbom_paths`, since the names move to `discovery.py`. Keep `Language` for frameworks and the OpenAPI commands only. | C |
| `services/sbom_service.py:58-75` | Import the names from `discovery.py`. | C |
| `services/sbom_service.py:182-216`, `232-345` | Repository-keyed output. Cache key without ref. Per-directory lock merge. No `language` argument. | B, C |
| `core/sandbox.py:155-242` | Recipes keyed by ecosystem. `recipes_for(discovery) -> [(dir, recipe)]`. | C |
| `commands/sbom/lock.py:71-176`, `commands/sbom/generate.py:73-215` | No language loop. Per-directory resolution. `RecordStore.remember` without a per-language ledger (`generate.py:115-119`). | C |
| `core/manifest.py:54-57` | `VENDOR_DIRS` comes from `discovery.EXCLUDED_DIRS`. | C |
| `core/manifest.py:490-570`, `664-782` | Ecosystem-keyed registry. `relationships_from(manifests) -> dict`. `_parse_gradle`/`_parse_pom` keep the group. Version-catalog resolution. | D |
| `services/db_service.py:171-197` | `scans_in` yields `(id, source, sha)`. | D |
| `services/db_service.py:199-352` | Master on the ledger. Per-ecosystem classification. Manifest-source rows (§4.7). Depgraph stamp. Remove `_depgraph_paths` (`298-322`). | D |
| `services/db_service.py:356-402` | Add `ecosystems`, `depgraph_ref`, `depgraph_commit_sha`, `depgraph_observed_at`, `github_language`, `language_bucket`. | D |
| `commands/db/index.py:30-261` | Remove `--language`. Add `--repos-file`. Incremental by default. | D |
| `core/documents.py:59`, `262-278` | Prefix depth 4, relative paths. | B |
| `core/documents.py:354-437` | `RawRecords.records()` with no language. `_newest` without the suffix filter. | B |
| `core/documents.py:497-605` | `RecordStore.remember(record)`. `stage_input` reads the ledger. | B |
| `commands/db/raw.py:52-104`, `154-320` | Walk the ledger. Store relative paths plus `ref`/`commit_sha`. Add the `content-index` kind. | B |
| `core/schema.py:11-47` | `repositories`: the new columns. `languages` (line 43, never filled) is dropped. | D |
| `core/schema.py:183-191` | `raw_documents`: add `ref` and `commit_sha`. | B |
| `core/schema.py` | Add the `current_artifacts` view. | D |
| `core/repository.py:358-398`, `419-425` | Source-aware `forget_scans`. Read the view. | D |
| `core/rollups.py:1-514` | §4.12 | E |
| `export/queries.py:25-190`, `export/schema.py:122-125`, `export/d1.py:160-966` | §4.13 | E |
| `web/src/**` | §4.13 | E |
| `core/prune.py:15-153` | `SCAN_DEPTH = 3`. `RepoKey = (owner, repo)`. Never touches `09-github-depgraph`. | B |
| `commands/data/prune.py:51-53` | Same stage list, new layout. | B |
| `commands/data/slim.py:83-93` | Operates on `index.jsonl`. No-op once the per-language ledgers are archived. | B |
| `services/openapi_service.py:234-236` | `tree_file(o, r, sha)` | B |
| `services/search_service.py:180` | Fix `language:None`. | F |
| `commands/github/search.py:65-95` | Write `all-<date>.jsonl`. | F |
| `services/release_service.py:76-147`; `services/git_service.py` | Tag dating with `git fetch --filter=tree:0`. Capped API fallback. | F |
| `commands/data/migrate_layout.py` (new) | §7 | B |
| `deploy/collector-loop.sh:105-140` | A depgraph worker loop beside the core worker. `track --snapshot`. | A, B |

---

## 6. Tests

The suite has 925 `test_` functions. Seven files need ClickHouse and skip without it (`tests/conftest.py`).

**Rewrite: they encode the language-keyed shape**

| File | Tests |
|---|---|
| `content_test.py` | `:31`, `:70`, `:112`. Rewrite for discovery input and repository-keyed paths. |
| `language_test.py` | The `test_*_paths` tests (`:28-75`). Replace with `discovery_test.py`. |
| `sbom_test.py` | `:36`, `:78` |
| `sbom_generate_test.py` | Fixtures `CONTENT`/`LEDGER`/`_project`/`_sbom` (`:36-37`, `FakeSyft .parts[-3]` at `:110`). All tests from `:215` to `:336` are otherwise kept. |
| `sbom_lock_test.py` | The helpers at `:84-99`. Tests `:181`, `:208`, `:229`, `:252`, `:310`, `:330`, `:346`, `:362`, `:391`. |
| `sandbox_test.py` | `:117`, `:124`, `:129`, `:150` |
| `syft_cache_test.py` | All three |
| `depgraph_command_test.py` | Fixtures at `:42-43`, `:147`. `:229` asserts `['java.jsonl']`, and every test from `:198` to `:532` depends on these fixtures. |
| `run_service_test.py` | `:217` (`content_path`), plus the `_track` helper. |
| `tree_command_test.py` | `:85`, `:93`, `:113` |
| `prune_test.py` | `:16`, `:26`, `:40`, plus the fixtures of the tests at `:48-131`. |
| `index_command_test.py` | `:19`, `:28`, `:57`, `:74` (`--language` → `--repos-file`) |
| `documents_test.py` | The prefix-depth tests at `:209`, `:246`, `:336`, `:361`, `:380`, `:400`. The RawRecords scoping tests at `:482`, `:511`, `:537`, `:558`, `:571`, `:586`, `:602`. `:511` and `:537` are deleted, because what they protect (ledger-path scoping) is removed on purpose. |
| `manifest_test.py` | Every `parser_for(Language.X)` / `resolve_relationships(…, Language.X)` call switches to ecosystem keys. `:759` becomes "every ecosystem has a parser". The assertions are unchanged. |
| `db_ingest_test.py` | `:39` helper, `:287`, `:324`, `:370` (inverted: an unknown *repository language* no longer matters), `:420`, `:460` |
| `ledger_test.py` | `:267` is replaced by per-stage lease tests. |
| `sync_test.py` | `:220` |
| `rollups_test.py` | `:48-84` |
| web tests | `clickhouse.test.ts`, `d1queries.test.ts`, `queryview.test.tsx:88`, `overview.test.tsx:57`, `108`, `askdisclosure.test.tsx:62`, `sourceshares.test.tsx:19-38`, `tools.test.ts:144-148`, `291`, `scale.test.tsx:35` |

**New**

- **`discovery_test.py`**
  - The #51 cases as literal trees: `jeecg-boot/pom.xml`, `application/build.gradle`, `app/client/package.json`, and Stirling's `app/common/build.gradle`.
  - Exclusions, including `vendor/modules.txt` as the exception.
  - Cap ordering, and multi-ecosystem repositories.
- **`migrate_layout_test.py`**
  - The plan on a synthetic tree: counts, bytes, conflicts and case collisions.
  - Resume after a kill mid-journal.
  - Rollback restores byte-identical trees.
  - Refuses to cross filesystems.
- **`stage_state_test.py`**
  - Due by version, by input key and by clock.
  - Independent leases per stage.
  - A depgraph 404 is negative-cached for 30 days, and a 500 backs off DEPGRAPH only.
  - The closing date makes DEPGRAPH never due.
- **`classification_test.py`**: a TypeScript-labelled repository with `pom.xml` gets Maven verdicts; a Maven artifact with no Maven manifest is `unknown`.
- **`current_artifacts_test.py`** (ClickHouse)
  - Depgraph rows at their own sha are current.
  - A Syft re-ingest does not forget the depgraph rows.
  - Whole-corpus counts equal `uniqExact`.
- **`manifest_source_test.py`**: Gradle `implementation 'org.springframework.boot:spring-boot-starter-web'` gives a `source='manifest'` row with its purl; a `pom.xml` gives no manifest-source row.
- **`release_tags_git_test.py`**: dating from a local bare repository with annotated and lightweight tags; the fallback cap.

**Kept unchanged** (per the audit): `raw_documents_test`, `dependency_graph_test`, `dir_hash_test`, `ecosystems_test`, `export_*` (after the schema update), `conditional_test`, `github_release_test`, `release_service_test`, `security_test`, `systemd_units_test`, and the rest listed in the audit.

A guard test in `cli_surface_test.py` asserts two things: no command has a `--language` option except `github search`, and no `PathConfig` method takes a `language` parameter.

---

## 7. Data migration

It moves files and rewrites paths; it never fetches. Everything lives on `/dev/mapper/hdd-tank` (ext4). `data/` and `.cache/` resolve to the same filesystem (`/mnt/hdd-tank/chatsbom/{data,.cache}`), so every move is `rename(2)`: O(1), atomic, and with no extra space. The command refuses to run if `st_dev` differs between a source and its destination.

### 7.1 What moves

| Root | Units | Bytes | Rule |
|---|---|---|---|
| `05-github-tree` | 34,630 scan dirs | 2.0 GB | `<lang>/o/r/<ref>/<sha>` → `o/r/<sha>` |
| `06-github-content` | 34,630 scan dirs (28,069 non-empty, 46,425 files) | 4.0 GB | same |
| `07-sbom` | 28,069 scan dirs | 9.9 GB | same |
| `09-github-depgraph` | 24,946 documents | 9.9 GB | `<lang>/o/r/sbom.spdx.json` → `o/r/legacy/sbom.spdx.json`, plus a generated `meta.json` with `fetched_at` = `creationInfo.created` and `commit_sha = ''` |
| `10-generated-lock` | 1,311 dirs (1,127 files) | 0.2 GB | `<lang>/o/r/<sha>` → `o/r/<sha>/` (root lockfiles stay at the root) |
| `.cache/syft/1.41.2` | 1,317 files | — | drop the `<ref>/` level; identical hashes collapse |
| `.cache/syft/<owner>` (legacy, unversioned) | 30,691 files | about 10 GB | **not read by current code** (F17). Moved to `.cache/syft/_unversioned/` and left there. Deleting it is open decision D6. |
| `.cache/git-tree` | 34,649 trees | 2.6 GB | drop the `<ref>/` level |
| `*/<lang>.jsonl` ledgers | 8 per stage | small (slimmed) | Moved to `<stage>/_legacy-lists/<lang>.jsonl`. Nothing reads them after the cutover. |
| `01-github-search/all.jsonl` | 1 | — | Renamed to `all-2026-03-09.jsonl`. The language lists go to `_legacy-lists/`. |

Ten repositories have two scan directories in 05 and 06:

- **Seven have two different shas.** One example is `aliasvault/aliasvault`, whose second scan is a branch that the release bug chose. These become two sha directories. The branch scan is then pruned by `data prune --keep 1` after the release re-run.
- **Three have one sha under both `HEAD` and `main`.** These collapse into one directory. The plan compares the two trees file by file. If they are byte-identical, the `HEAD` copy moves to `_migration/dedup/`. If not, the plan stops with a conflict.

No `(owner, repo, sha)` exists under two languages, and the plan re-checks this.

### 7.2 Steps

Order matters. Code and layout switch together, so the collector is stopped for the window.

1. **Freeze writers.**
   - Run `docker compose stop collector`.
   - Confirm that no `chatsbom` processes remain (`pgrep -f chatsbom`).
   - The web keeps serving from D1 and is unaffected.
2. **Snapshot.**
   - `sqlite3 data/ledger.sqlite3 ".backup data/_migration/ledger.pre.sqlite3"`.
   - Copy the ledger JSONL files.
   - `ALTER TABLE raw_documents FREEZE WITH NAME 'pre_layout'`, and the same for `artifacts` and `repositories`. These are hard-link snapshots, so they cost no space until parts diverge.
3. **Inventory before.** `chatsbom data migrate-layout --inventory > data/_migration/pre.tsv`. One line per file: `stage`, relative path, size, mtime. Plus per-stage totals: files, bytes, and scan dirs.
4. **Dry run.** `chatsbom data migrate-layout` (the default is dry-run) writes `data/_migration/plan.tsv` (`src`, `dst`, `kind`) and reports:
   - moves per root;
   - destination collisions (expected 0);
   - case-insensitive `owner/repo` collisions (expected 0);
   - cross-device moves (expected 0);
   - the planned `raw_documents` path rewrites by kind.

   It aborts on any non-zero conflict unless `--resolve newest` is passed. With that flag, the older conflicting source goes to `_migration/conflicts/`.
5. **Apply.** `chatsbom data migrate-layout --apply` executes the plan:
   - Before each `rename`, it appends `BEGIN src dst` to `data/_migration/journal.tsv`, fsynced; after, `DONE`.
   - An interrupted run resumes from the journal. A `BEGIN` without a `DONE` is checked: if `dst` exists and `src` does not, it is marked done; otherwise it is retried.
   - Empty parent directories left behind (`<lang>/o/r/<ref>`) are removed afterwards.
6. **Rewrite `raw_documents.path`** with one mutation per kind, using the same deterministic mapping as the plan, as a regex:
   - `^.*?/(05-github-tree|06-github-content|07-sbom)/(go|java|javascript|php|python|ruby|rust|typescript|node)/([^/]+)/([^/]+)/[^/]+/([0-9a-f]{40})(/.*)?$` → `\1/\3/\4/\5\6`;
   - the depgraph `…/09-github-depgraph/<lang>/o/r/sbom.spdx.json` → `09-github-depgraph/o/r/legacy/sbom.spdx.json`.

   The ALTERs add the `ref` and `commit_sha` columns first, then fill `commit_sha` for SYFT and CONTENT from the sha captured by the same regex. `path` is not part of the sort key, so the mutation rewrites only the `path` column parts. The body column is untouched. That column is why this is minutes, not an hour.
7. **Ledger.** In a transaction:
   - create `stage_state` from `stage_watermarks`, with `stage_version = 1`, `outcome = ok` and empty keys;
   - set `github_language` from the newest `repo-metadata` in `raw_documents`;
   - set `language = ''` on every row;
   - bump no versions yet. Versions are bumped at rollout, not by the migration, so the migration alone changes no behaviour.
8. **Inventory after, then verify.** `chatsbom data migrate-layout --verify` checks:
   - Per root, file count and total bytes after equal before, minus explicitly listed dedups (collapsed Syft cache hashes, which must be byte-identical).
   - Every `dst` in `plan.tsv` exists, and no `src` does.
   - A 1% random sample of files has an identical sha256 at `src` (from `pre.tsv`, hashed in step 3 for the sample) and at `dst`.
   - In `raw_documents`: `count()` per kind is unchanged, `countIf(path LIKE '%/go/%' …) = 0` for the rewritten kinds, and every SYFT and CONTENT `path` resolves to a file.
   - **A transform equivalence check.** Run the new code's `db index --rebuild` into a scratch database (`CLICKHOUSE_DB=chatsbom_migration_check`). Compare `SELECT repository_id, source, count(), uniqExact(name)` with production. It must be identical for 100% of repositories. The only permitted differences are depgraph `sbom_commit_sha` values, which become `''` by design (§4.11). This runs before any `STAGE_VERSION` is bumped, so the collected data is exactly what it was.
9. **Swap in.** Rebuild the production tables from `raw_documents` (`db index --rebuild`), then refresh the rollups and run `verify_rollups.py`. D1 export is deferred until PR E lands.

**Rollback.** Available at any point before new collection starts, which is step 1 of the rollout (§8.2).

- `chatsbom data migrate-layout --rollback` replays `journal.tsv` in reverse: `rename(dst, src)`, then recreates the removed empty directories from `plan.tsv`.
- Restore `ledger.pre.sqlite3` and the JSONL files.
- `raw_documents`: run the inverse path mapping as a mutation. As a last resort, copy the frozen parts from `shadow/pre_layout/` into the table's `detached/` directory and `ALTER TABLE … ATTACH PART` each one.
- Check out `de60629` and restart the collector.
- The same `--verify` then runs against `pre.tsv`.

**Time.** The moves are about 160 k renames plus 32 k cache files on an HDD, and should take under 10 minutes. The inventory and hashing of the 1% sample take about 5 minutes. The `raw_documents` mutation takes minutes. The scratch `db index --rebuild` is on the order of a full index pass, which is currently minutes. Plan a 2-hour window.

---

## 8. Rollout

### 8.1 PRs, in order

The depgraph deadline sets the order. PR A has to be collecting within about a week.

| PR | Content | Why this order |
|---|---|---|
| **A** | Depgraph independence (§4.8): negative cache, permanent repository-keyed store, closing date, separate worker in `collector-loop.sh`. Runs over the ledger's tracked set, and also over `all-2026-03-09.jsonl` until the search refresh. Includes `track --snapshot`, limited to seeding. | About 40 k graphs at about 100 per hour is about 17 days, and the endpoint closes 2026-11-13. Every other change can wait; this one cannot. It writes only new paths (`09-github-depgraph/<o>/<r>/…`), so it does not need the full migration. |
| **B** | Layout plus migration command, ledger v2 (`stage_state`, per-stage claims, `--stage`, `--repos-file`), readers (`documents.py`, `db raw`), prune, Syft cache key. **Behaviour-neutral**: no version bumps. | Makes the migration and its equivalence check possible on unchanged semantics. |
| **C** | Discovery, content, SBOM over everything, lock per directory. `STAGE_VERSION[CONTENT] = SBOM = LOCK = 2`. | |
| **D** | Ecosystem classification, `manifest` source (if D1 is approved), depgraph stamping, `current_artifacts`, `db index` masters on the ledger. | |
| **E** | Rollups, D1 export, web. | Can be reviewed in parallel with D. It depends on D's schema. |
| **F** | Search `language:None` fix, snapshot, git-based tag dating, `STAGE_VERSION[RELEASE] = 2`. | Small. Can land any time after B. |

Every command in the rollout runs with `GITHUB_TOKEN=$(gh auth token)`, because the `.env` token is invalid. The collector's `.env` is updated before PR A is deployed.

### 8.2 Pilot: about 100 repositories

The pilot runs after the migration (B), with C, D and F deployed. `data/_pilot/pilot.txt` (`owner/repo` per line) contains:

- **The six named repositories:** jeecgboot/JeecgBoot, halo-dev/halo, Stirling-Tools/Stirling-PDF, appsmithorg/appsmith, mathesar-foundation/mathesar, WebGoat/WebGoat.
- **30 from `spring.csv` not currently in the Spring pool.** Stratified: 15 Gradle, 10 subdirectory Maven, 5 whose GitHub language is not Java.
- **15 from `chi.csv` not currently found.**
- **20 of the 2,300 "below the root only" repositories**, 2 or 3 per former language.
- **10 multi-ecosystem repositories**, including one over the 200-file cap.
- **10 from languages never collected before:** None, C++, C#, Kotlin, Swift, Shell, HTML.
- **9 unchanged controls** with a root manifest and an SBOM today, for regression.

Commands:

```
chatsbom queue track --snapshot data/01-github-search/all-<date>.jsonl
chatsbom run --repos-file data/_pilot/pilot.txt --limit 100 --quota 2000
chatsbom run --stage depgraph --repos-file data/_pilot/pilot.txt --quota 100
chatsbom db raw --apply --repos-file data/_pilot/pilot.txt
chatsbom db index --repos-file data/_pilot/pilot.txt
python scripts/pilot_report.py data/_pilot/pilot.txt   # new, read-only
```

`pilot_report.py` prints, per repository:

- ecosystems;
- discovered, selected and skipped files, with reasons;
- bytes;
- Syft artifact count before and after;
- depgraph outcome;
- the share of direct, transitive and unknown verdicts;
- whether `spring-boot-starter-web` / `go-chi/chi` is present, and from which source;
- the download target, with its ref and ref type;
- API and raw requests spent, and wall time per stage.

The pilot passes when:

- every §10 criterion that applies to its repositories holds;
- the controls' artifact sets are unchanged, except for new subdirectory manifests;
- no stage exceeds 2x the per-repository time assumed in §9.

The pilot's measured per-repository costs replace the estimates in §9 before the full run.

### 8.3 Full run

1. Search refresh (F): about 1 hour. Then `queue track --snapshot`.
2. Start the worker processes. They are all separate so that each is paced by its own limit:
   - `run --stage release,commit` on the core API, at 4,000 per hour;
   - `run --stage tree,content` on git and raw, with 4 workers;
   - `run --stage lock` in Docker, with 2 workers;
   - `run --stage sbom` with Syft, with 4 workers;
   - `run --stage depgraph`, already running since PR A.
3. Run `db raw --apply` and `db index` every 6 hours during the run, then back to daily.
4. Once SBOM coverage is flat, export to D1 behind the E release and check the acceptance criteria (§10).
5. Return the collector to its loop. `collector-loop.sh` gains the per-stage workers.

---

## 9. Estimates

The corpus after the refresh is taken as about **65 k** repositories: 60,017 at 1,000 or more stars in March, plus growth. The pilot replaces every per-repository figure below with a measured one.

### 9.1 GitHub budget

| Bucket | Work | Requests | Time |
|---|---|---|---|
| Search (30 per minute) | Unfiltered, at least 1,000 stars, star-sliced | about 1–2 k | about 1 h |
| Core REST (5,000 per hour) | Releases: 1.16 pages per repository × 65 k | about 75 k | about 19 h at 4,000 per hour |
| | Tag dates | 0 with git dating. The capped fallback would be about 6.1 per repository, about 400 k, 100 h. | — |
| | Repository metadata | 0 extra. The search payload carries it, and `queue sync` afterwards is conditional, with 304s free. | — |
| Depgraph (about 100 per hour) | Never fetched: 65 k − 24,946 ≈ 40 k | 40 k | about **17 days** |
| | Refreshing the 24,946 legacy graphs | 25 k | about 10.4 days more |
| raw.githubusercontent.com (not core) | Existing corpus after the cap: 229,721 files. New repositories: about 30 k × 6.6 ≈ 200 k. | about 430 k GETs | about 15–30 h at 4–8 per second |
| git (not metered) | `ls-remote` and tag fetch: 65 k. Blobless clones for trees: about 30 k new, plus re-clones for shas that changed. | about 100 k ops | about 10 h with 4 workers |

**Deadline check for the depgraph.** From PR A going live, the never-fetched set takes about 17 days. Today is 2026-09-28 and the endpoint closes after 2026-11-13, 46 days away. If A is live by about 2026-10-10, both the new graphs and one refresh of the legacy graphs (about 27 days in all) finish with a margin of about 9 days. If A slips past about 2026-10-27, even the never-fetched set is at risk. The legacy graphs are already stored permanently, so skipping their refresh loses freshness, not coverage.

### 9.2 Wall time for the non-depgraph work

| Phase | Estimate |
|---|---|
| Migration window | about 2 h |
| Pilot, including review | about 1 day |
| Search refresh plus track | about 1–2 h |
| Release and commit (token-bound) | about 19 h |
| Tree and content (overlapping) | about 15–30 h |
| Lock (about 1.5 k directories; Docker at about 30 s each, 2 workers) | about 6 h |
| Syft: 65 k × about 3 s with 4 workers (content roots grow from 1.7 to about 6.6 files each) | about 14 h |
| `db raw` plus `db index --rebuild` plus rollups plus verification | about 2–3 h |
| **SBOM side, end to end** | **about 3 days after the pilot**, with the stages overlapping |

### 9.3 Disk (`/mnt/hdd-tank`: 304 GB free)

| Item | Today | After (worst case) | Increase |
|---|---|---|---|
| `06-github-content` | 4.0 GB | about 13 GB (existing, estimated from the average bytes per file name) + about 9 GB (new) | **about +18 GB** |
| `07-sbom` (about 350 KB per repository, × 1.5 for more manifests) | 9.9 GB | about 29 GB | +19 GB |
| `.cache/syft/1.41.2` (mirrors 07) | small | about 29 GB | +29 GB |
| `09-github-depgraph` (about 400 KB per document; history deduplicated) | 9.9 GB | about 26 GB + about 10 GB of refreshes | +26 GB |
| `05-github-tree` + `.cache/git-tree` | 4.6 GB | about 8.6 GB | +4 GB |
| ClickHouse (`raw_documents` plus about 2x `artifacts` rows) | about 3 GB | about 13 GB | +10 GB |
| **Total** | | | **about +105 GB**, leaving about 200 GB free |

Reclaimable if needed:

- `.cache/syft/_unversioned`, about 10 GB, never read (open decision D6);
- `.cache/git-tree`, 2.6 GB, a duplicate of 05;
- `.requests-cache/db.sqlite3`, 30 GB, all expired past its 7-day TTL;
- `data prune --keep 1` on 05, 06 and 07 once the new scans exist.

---

## 10. Acceptance criteria

1. **Spring parity.** Take the repositories in `notes/selection-pool/spring.csv` (720 found through GitHub code search on 2026-09-27) that are in the refreshed snapshot, with at least 1,000 stars and not fork, archived, template or mirror. At least **95%** of them have an artifact named like `spring-boot-starter*` in `current_artifacts`, from any source. Today it is 387 of 720. Each miss is listed in `pilot_report.py --pool spring` with a reason code:
   - `not-in-snapshot`;
   - `evidence-only-at-HEAD` (the scan target is an older release);
   - `catalog-unresolved`;
   - `over-cap`;
   - `syft-timeout`;
   - `depgraph-absent-and-gradle`, when D1 is declined.

   No miss may be `unexplained`.
2. **Chi parity.** The same check against `chi.csv` (260; 191 today), on `github.com/go-chi/chi*` rows. At least 95%, no `unexplained`.
3. **Named repositories.** jeecgboot/JeecgBoot, halo-dev/halo, Stirling-Tools/Stirling-PDF and appsmithorg/appsmith each:
   - have a `repositories` row;
   - have `ecosystems ⊇ {maven}`, and also `npm` for appsmith and Stirling-PDF;
   - have `spring-boot-starter-web` in `current_artifacts`.

   halo meets this through `source = 'manifest'` if D1 is approved. If it is declined, halo is a documented exception.
4. **Release targets.** For mathesar-foundation/mathesar and WebGoat/WebGoat:
   - `sbom_ref_type` is `release` with a ref present under `refs/tags/`, or `branch` equal to `default_branch`;
   - no corpus repository has `sbom_ref_type = 'release'` with a ref that is not a tag. This is checked with a `git ls-remote` sample of 500.
5. **Coverage.** Of the repositories whose stored tree has at least one discoverable manifest, 99% have a non-empty content root. The rest are recorded failures with a reason. The #51 figure of 19% with no manifest drops to the share of repositories with no discoverable manifest at all: 3,516 of 34,620, about 10%, today.
6. **Depgraph.** Before 2026-11-13, every tracked repository in the snapshot has a DEPGRAPH `stage_state` row with outcome `ok`, `absent`, `too_large` or `failed` (with its error). After that date, a full `run` completes with DEPGRAPH `closed` and produces the same Syft and manifest rows.
7. **No language keys.** The `cli_surface_test` guard (§6) passes, and `grep -rn "get_.*_list_path(.*lang" chatsbom` is empty.
8. **Migration.** `--verify` passes. The scratch-database equivalence check is identical for 100% of repositories. The journal is complete, with no `BEGIN` lacking a `DONE`.
9. **Rollups.**
   - `verify_rollups.py` passes all checks, now per ecosystem.
   - `benchmark_queries.py` totals at most 50 ms. It is 43.9 ms today.
   - `totals().dependencies` agrees between ClickHouse and D1, as TODO.md §G requires today.
10. **Tests.** The full suite and the web tests are green, including the new files listed in §6.

---

## 11. Risks

| Risk | Mitigation |
|---|---|
| The depgraph endpoint closes early, or its limit tightens below about 100 per hour. | PR A goes first. The worker fetches never-seen repositories first. Stored documents are permanent. Nothing depends on the endpoint. |
| Syft runtime grows sharply on monorepos (up to 200 manifests). | The 600 s timeout is recorded per repository, and depgraph and manifest rows still land. The pilot measures p99. The cap can be lowered per ecosystem. |
| Headline numbers shift. The direct share changes, and new ecosystems and repositories appear. | Publish the rollup version and snapshot id on the dashboard. Keep `all-2026-03-09` reproducible, because `artifacts` history keeps the old scans. |
| Double counting across ecosystems. | Whole-corpus figures are `uniqExact` over `current_artifacts` only. `verify_rollups.py` asserts this. |
| The migration is interrupted, or a path mapping is wrong. | fsynced journal, resume, rollback, `FREEZE` snapshots. The dry run must report 0 conflicts. The scratch-database equivalence check runs before production is touched. |
| `raw.githubusercontent.com` throttling at about 430 k GETs. | 2–4 workers with backoff on 429, and switch to the batched git fetch (§4.5) if the pilot sees throttling. |
| Excluding `examples/` and `samples/` drops real projects that are collections of examples, such as `java-design-patterns` (16 matched build files in the Spring pool). | Open decision D5. The discovery list records every skipped path and its reason, so the effect is measurable in the pilot. |
| Untrusted inputs: more XML and TOML parsed, and more lock recipes run on subdirectories. | Use `defusedxml` for `pom.xml` (currently `xml.etree`). Keep the existing Docker sandbox limits, capped at 10 directories per repository. |
| Renames and case changes between the snapshot and collection. | Detected by `repository_id` in `queue sync`, with a journaled directory move (§4.3). |
| D1 and web breaking for cached clients. | The Worker accepts `language` for one release (§4.13). |
| Disk | About +105 GB worst case against 304 GB free, plus about 43 GB of known reclaimable caches (§9.3). |

---

## 12. Decisions the owner still needs to make

- **D1. Declared-manifest artifact rows (`source = 'manifest'`) for Gradle build files and version catalogs.** Without them, Gradle-only repositories with an incomplete GitHub graph can never show their Spring starters. halo is the named example. Syft 1.41.2 produced nothing for 40 of 40 sampled Gradle projects. This adds a third source to `artifacts` and to every source split. *Recommendation: approve, limited to files Syft does not read.*
- **D2. Corpus membership after the refresh.** A repository in the old snapshot that is now below 1,000 stars, deleted, archived or private: keep it in the rollups, or report only the current snapshot? *Recommendation: rollups and D1 cover the current snapshot only; the history stays in `artifacts`.*
- **D3. Path key for repositories:** `owner/repo` as GitHub spells it (readable, but moves on rename) or the numeric `repository_id` (stable, opaque). *Recommendation: `owner/repo`, as in the decided example, with rename moves in `queue sync`.*
- **D4. Depgraph throughput.** One token at about 100 per hour finishes the never-fetched graphs in about 17 days. Should a second credential be used (a GitHub App installation, or another account's token) to add margin before 2026-11-13? And should the refresh of the 24,946 legacy graphs be attempted at all? *Recommendation: one token; never-fetched first; refresh only with time left.*
- **D5. `examples/`, `samples/`, `demo/`.** Skip them, as decided, accepting that repositories which are collections of examples lose their build files? Or download them tagged `scope=example` and leave them out of the default rollups? `docs/` is proposed as not skipped.
- **D6. Deleting `.cache/syft/<owner>/…`**: 30,691 unversioned Syft cache entries, about 10 GB, that current code never reads. The migration only moves them aside.
- **D7. Language fold.** The top 12 GitHub languages plus `other`, or a share threshold such as at least 1% of repositories?
- **D8. Scan target versus the cross-check.** The pools check the default-branch HEAD, while the pipeline scans the latest stable release (ruling C15). Acceptance counts `evidence-only-at-HEAD` as an explained miss. Confirm that this is acceptable rather than scanning HEAD as well.

---

## Appendix: measurements

All read-only, on 2026-09-28.

- **Discovery figures (§4.4, §9.3).** Every `05-github-tree/*/…/tree.txt` was read, newest per repository, 34,620 in all. Each path was matched against `MANIFEST_NAMES ∪ MANIFEST_SUFFIXES ∪ {environment.yml, requirements-dev.txt}` with the §4.4 exclusions. The byte estimate multiplies each matched file name by that name's mean size in the current `06-github-content`: 92,257 bytes over all names.
- **Layout counts (§7.1).** `find -mindepth 5 -maxdepth 5 -type d` per stage, and `du --apparent-size`. Collisions: grouped by `(owner, repo)` case-insensitively and by `(owner, repo, sha)` across languages. 0 of each.
- **Release caches (F19).** All 34,728 `.cache/api.github.com/repos/*/*/releases/index.json` files: 1,493 of a 1,500 sample are bare lists, the rest `version: 1`, and none are `version: 2`. Tags without a release were counted in the 1,500 sample from `git/refs/index.json` `refs/tags/*`: mean 47.4, median 1, p99 662. The per-repository mean capped at 20 is 6.11. Release pages: mean 1.16.
- **Syft and Gradle (F22).** 40 of the first `06-github-content/java/*` scans with `build.gradle` and no `pom.xml` whose `07-sbom` exists: all 40 have `artifacts: []`, with Syft 1.41.2.
- **Ledger (F23).** `sqlite3 'file:data/ledger.sqlite3?mode=ro&immutable=1'`.
- **Spring and Chi pools (F24).** `notes/selection-pool/{spring,chi}.csv` in the paper repository, matched against `01-github-search/all.jsonl` and the eight language lists.
