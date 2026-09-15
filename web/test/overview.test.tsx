/**
 * The overview's one filtered panel, and whether its heading tells the
 * truth about what it is showing.
 *
 * `Most declared packages` had a fixed title and qualifier while the
 * ranking underneath followed a checkbox. Clearing it left the panel
 * headed "by repositories that declare them" above semver, debug and
 * ms — which its own note calls npm utilities nobody chooses by name.
 * This page exists to separate declared from inherited, so that panel
 * stated the argument backwards.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe as group, expect, it, vi } from 'vitest';
import { Overview } from '../src/components/Overview';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;
import type { DatasetClient } from '../src/d1/client';

beforeEach(() => cleanup());

/** Declared-only and unfiltered give genuinely different rankings. */
const DECLARED = [
  { name: 'typescript', repositoryCount: 6863, directCount: 6820 },
  { name: 'eslint', repositoryCount: 6440, directCount: 6401 },
];
const ALL = [
  { name: 'semver', repositoryCount: 9619, directCount: 2458 },
  { name: 'debug', repositoryCount: 9027, directCount: 812 },
];

/**
 * Answers every method the panel needs and nothing more. Unlisted keys
 * resolve empty rather than throwing, because this test is about one
 * panel's heading and the others may render however they like.
 */
function client(): DatasetClient {
  return new Proxy({}, {
    get(_t, key: string) {
      return (...args: unknown[]) => {
        if (key === 'topPackages') {
          const query = (args[0] ?? {}) as { directOnly?: boolean };
          return Promise.resolve(query.directOnly ? DECLARED : ALL);
        }
        if (key === 'relationshipSplit') {
          return Promise.resolve({ direct: 1, transitive: 4, unknown: 0 });
        }
        return Promise.resolve([]);
      };
    },
  }) as DatasetClient;
}

const mount = () =>
  render(<Overview words={EN} locale="en" dataset={client()} languages={['php']} go={vi.fn()} />);

/**
 * The ranking panel's heading, in whichever language is rendered.
 *
 * Matched against the dictionary rather than an English pattern: the
 * first version hardcoded `/Most (declared|depended-on) packages/` and
 * silently found nothing in Chinese, so the assertion compared against
 * an empty string and said so unhelpfully.
 */
const heading = (words = EN): string => {
  const titles = [words.rankingTitleDeclared, words.rankingTitleAll];
  const node = [...document.querySelectorAll('h2, h3')].find((h) =>
    titles.some((title) => (h.textContent ?? '').includes(title)),
  );
  return node?.parentElement?.textContent ?? '';
};

group('Most declared packages', () => {
  it('claims "declared" while the filter is on', async () => {
    mount();
    await waitFor(() => expect(screen.getByText('typescript')).toBeTruthy());
    expect(heading()).toContain('Most declared packages');
    expect(heading()).toContain('by repositories that declare them');
  });

  it('stops claiming "declared" once the filter is cleared', async () => {
    mount();
    await waitFor(() => expect(screen.getByText('typescript')).toBeTruthy());

    const box = document.querySelector<HTMLInputElement>(
      'input[type=checkbox]',
    );
    expect(box?.checked).toBe(true);
    fireEvent.click(box!);

    // The ranking changes to the inherited-heavy one...
    await waitFor(() => expect(screen.getByText('semver')).toBeTruthy());
    // ...so the heading must stop saying these repositories declare it.
    expect(heading()).not.toContain('Most declared packages');
    expect(heading()).not.toContain('by repositories that declare them');
    expect(heading()).toContain('Most depended-on packages');
    expect(heading()).toContain('declared or inherited');
  });

  it('translates the panel and keeps the filter honest in Chinese', async () => {
    /**
     * The heading follows the filter in both languages. Translating it
     * would be worthless if the Chinese copy said 「主动声明」 over an
     * unfiltered ranking — the same defect the English heading had.
     */
    render(<Overview dataset={client()} languages={['php']} go={vi.fn()} words={ZH} locale="zh" />);
    await waitFor(() => expect(screen.getByText('typescript')).toBeTruthy());
    expect(heading(ZH)).toContain('最常被主动声明的包');

    const box = document.querySelector<HTMLInputElement>('input[type=checkbox]');
    fireEvent.click(box!);
    await waitFor(() => expect(screen.getByText('semver')).toBeTruthy());
    expect(heading(ZH)).not.toContain('最常被主动声明的包');
    expect(heading(ZH)).toContain('最多仓库依赖的包');
  });
});
