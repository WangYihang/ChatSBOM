/**
 * One name per ecosystem, because the two collectors disagree.
 *
 * Syft and GitHub's dependency graph label the same ecosystem
 * differently, and nothing reconciled them — so the ecosystem filter
 * offered `composer · 183` and `php-composer · 97` for
 * `laravel/framework` as though Composer were two ecosystems, and the
 * counts were each a fraction of the truth. Measured across the whole
 * corpus:
 *
 *     depgraph        syft             one ecosystem
 *     cargo           rust-crate       cargo
 *     pypi            python           pypi
 *     golang          go-module        go
 *     maven           java-archive     maven
 *     composer        php-composer     composer
 *     npm             npm              npm
 *     gem             gem              gem
 *
 * The rest are reported by one collector only and need no mapping.
 *
 * The canonical name is the one a person would recognise from the
 * registry — `cargo`, not `rust-crate`; `pypi`, not `python` — since
 * this is what the interface shows.
 */

/** Raw `artifacts.type` values, grouped under the name shown. */
const MEMBERS: Readonly<Record<string, readonly string[]>> = {
  npm: ['npm'],
  cargo: ['cargo', 'rust-crate'],
  pypi: ['pypi', 'python'],
  go: ['golang', 'go-module'],
  maven: ['maven', 'java-archive'],
  gem: ['gem'],
  composer: ['composer', 'php-composer'],
  pub: ['pub'],
  nuget: ['nuget'],
  swift: ['swift'],
  deb: ['deb'],
  'jenkins-plugin': ['jenkins-plugin'],
  swid: ['swid'],
};

/** Raw value -> the name shown. Built once from MEMBERS. */
const CANONICAL: ReadonlyMap<string, string> = new Map(
  Object.entries(MEMBERS).flatMap(
    ([canonical, raw]) => raw.map((value) => [value, canonical] as const),
  ),
);

/**
 * The name to show for a raw `type`.
 *
 * An unmapped value is returned unchanged rather than hidden or
 * bucketed: a new ecosystem appearing in the data should read as
 * itself, not as `other`, and that is also the signal that this table
 * needs a line adding.
 */
export function ecosystemName(type: string): string {
  return CANONICAL.get(type) ?? type;
}

/**
 * The raw `type` values a shown name covers.
 *
 * The filter has to expand back before it reaches `artifacts`, whose
 * rows still carry the collector's own spelling. Sending `go` straight
 * through matched nothing, which would read on the page as an
 * ecosystem with no dependants.
 */
export function ecosystemMembers(name: string): readonly string[] {
  return MEMBERS[name] ?? [name];
}

/** Whether a name is one this table knows. */
export function isKnownEcosystem(name: string): boolean {
  return name in MEMBERS;
}
