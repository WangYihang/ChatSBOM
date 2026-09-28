/**
 * The part of `DatasetQueries` both stores answer alike, written once.
 *
 * A store extends this with what only it can say — its point lookups,
 * its edges, its provenance — and hands it two things: which half of
 * `READS` is its own, and how it runs a statement written with
 * `{name:Type}` placeholders. Nothing else here knows which store it is
 * talking to.
 */
import type { DatasetQueries } from '../backend';
import { READS, type Read, shapeRead, statement, type Store, type Value } from './reads';
import { boundedLimit, type Row, walkTree } from './shape';
import type {
  AdoptionPoint,
  DependencyBucket,
  DependencyTree,
  EcosystemCoverage,
  LanguageCoverage,
  LicenseShare,
  PackageEdge,
  PackagePopularity,
  SourceComparison,
  Totals,
} from './types';

/** Runs one statement, binding its `{name:Type}` values as the store does. */
export type Reader = (
  sql: string,
  values: Readonly<Record<string, Value>>,
) => Promise<Row[]>;

type Shared =
  | 'totals'
  | 'languageCoverage'
  | 'ecosystemCoverage'
  | 'topPackages'
  | 'dependencyDistribution'
  | 'sourceComparison'
  | 'licenseShares'
  | 'adoptionOverTime'
  | 'dependencyTree';

export abstract class SharedDataset implements Pick<DatasetQueries, Shared> {
  protected constructor(
    private readonly store: Store,
    private readonly reader: Reader,
  ) {}

  private async read<T>(
    spec: Read<T>,
    values: Readonly<Record<string, Value>> = {},
  ): Promise<T[]> {
    const rows = await this.reader(statement(spec, this.store), values);
    return rows.map((row) => shapeRead(spec, row));
  }

  async totals(): Promise<Totals> {
    const [row] = await this.read(READS.totals);
    // One stored row; none is an empty dataset rather than a failure.
    return row ?? shapeRead(READS.totals, {});
  }

  languageCoverage(): Promise<LanguageCoverage[]> {
    return this.read(READS.languageCoverage);
  }

  ecosystemCoverage(): Promise<EcosystemCoverage[]> {
    return this.read(READS.ecosystemCoverage);
  }

  topPackages(options: {
    directOnly?: boolean;
    ecosystem?: string;
    limit?: number;
  }): Promise<PackagePopularity[]> {
    return this.read(READS.topPackages, {
      direct: options.directOnly ? 1 : 0,
      // The empty string is the whole-corpus row.
      ecosystem: options.ecosystem ? options.ecosystem.toLowerCase() : '',
      limit: boundedLimit(options.limit),
    });
  }

  dependencyDistribution(): Promise<DependencyBucket[]> {
    return this.read(READS.dependencyDistribution);
  }

  sourceComparison(): Promise<SourceComparison[]> {
    return this.read(READS.sourceComparison);
  }

  licenseShares(limit = 12): Promise<LicenseShare[]> {
    return this.read(READS.licenseShares, { limit: boundedLimit(limit) });
  }

  adoptionOverTime(name: string): Promise<AdoptionPoint[]> {
    return this.read(READS.adoptionOverTime, { name });
  }

  dependencyTree(
    name: string,
    options: { children?: number; branch?: number } = {},
  ): Promise<DependencyTree> {
    return walkTree(
      name,
      options,
      (root, limit) => this.dependenciesOf(root, limit),
      (root, children, branch) => this.secondHop(root, children, branch),
    );
  }

  /** The tree's first hop is this, bounded by the tree's own shape. */
  abstract dependenciesOf(name: string, limit?: number): Promise<PackageEdge[]>;

  /**
   * The tree's second hop: up to `branch` edges out of each of
   * `children`, never back to `root`, as `parent`, `child` and
   * `repositories`, widest first and then by child and parent — the
   * rows tie, and a diagram drawn from them should not move between
   * two loads of one page.
   */
  protected abstract secondHop(
    root: string,
    children: readonly PackageEdge[],
    branch: number,
  ): Promise<Row[]>;
}
