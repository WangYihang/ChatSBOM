/**
 * The sentence above the dependants table, on its own.
 *
 * It read "1 dependants on mail — 0 declare it, 1 inherit it": three
 * pluralisation faults in one line, and the suite asserted two of them
 * verbatim. Most packages in this dataset have a handful of dependants,
 * so the singular is the ordinary case, not an edge.
 */
import { describe as group, expect, it } from 'vitest';
import { count, statusLine } from '../src/components/QueryView';
import type { Dependent } from '../src/d1/queries';
import { DICTIONARIES } from '../src/i18n/strings';

const row = (relationship: 'direct' | 'transitive'): Dependent =>
  ({
    owner: 'o', repo: 'r', stars: 1, version: '1.0.0',
    relationship, observedAt: '2026-09-13',
  }) as unknown as Dependent;

const ready = (rows: Dependent[], total: number) =>
  ({ status: 'ready' as const, value: { rows, total } });

const line = (rows: Dependent[], total: number, directOnly = false) =>
  statusLine('mail', ready(rows, total), directOnly, '', EN);

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

group('count', () => {
  it('uses the singular for exactly one', () => {
    expect(count(1, 'dependant')).toBe('1 dependant');
  });

  it('uses the plural for none and for many', () => {
    expect(count(0, 'dependant')).toBe('0 dependants');
    expect(count(2, 'dependant')).toBe('2 dependants');
  });

  it('takes an irregular plural', () => {
    expect(count(3, 'repository', 'repositories')).toBe('3 repositories');
    expect(count(1, 'repository', 'repositories')).toBe('1 repository');
  });

  it('groups thousands, since these counts reach five figures', () => {
    expect(count(19502430, 'record')).toBe('19,502,430 records');
  });
});

group('statusLine', () => {
  it('agrees in number with a single dependant', () => {
    expect(line([row('direct')], 1)).toBe(
      '1 dependant on mail — 1 declares it, 0 inherit it.',
    );
  });

  it('agrees in number with a single inheritor', () => {
    expect(line([row('transitive')], 1)).toBe(
      '1 dependant on mail — 0 declare it, 1 inherits it.',
    );
  });

  it('keeps the plural when there is more than one', () => {
    expect(line([row('direct'), row('transitive')], 2)).toBe(
      '2 dependants on mail — 1 declares it, 1 inherits it.',
    );
  });

  it('says the split covers only the rows shown when capped', () => {
    // The real finding: the split is known for the page, not the total.
    expect(line([row('transitive')], 124)).toBe(
      '124 dependants on mail — 0 declare it, 1 inherits it among the 1 shown.',
    );
  });

  it('agrees in number in the declared-only sentence', () => {
    expect(line([row('direct')], 1, true)).toBe('1 repository declares mail.');
    expect(line([row('direct'), row('direct')], 2, true)).toBe(
      '2 repositories declare mail.',
    );
  });

  it('never claims a dependant when there are none', () => {
    expect(line([], 0)).toBe('No repository in the dataset depends on mail.');
  });

  it('never appends an English plural to a Chinese noun', () => {
    /**
     * `count` used to default `plural` to `${singular}s`, and a call
     * site that omitted the argument put 「326 个依赖方s」 on the page.
     * A comment in the function saying Chinese has no plural did not
     * prevent it, because nothing required the caller to read it.
     */
    const line = statusLine('mail', ready([row('direct')], 326), false, '', ZH);
    expect(line).not.toContain('s');
    expect(line).toContain('个依赖方');
  });

  it('assembles the Chinese sentence in Chinese word order', () => {
    // English leads with the count; Chinese leads with the subject.
    const line = statusLine('mail', ready([row('direct')], 326), false, '', ZH);
    expect(line.indexOf('mail')).toBeLessThan(line.indexOf('个依赖方'));
    expect(line).toContain('主动声明');
  });
});
