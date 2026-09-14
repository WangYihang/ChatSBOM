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
  render(<Overview dataset={client()} languages={['php']} go={vi.fn()} />);

const heading = (): string => {
  const node = [...document.querySelectorAll('h2, h3')].find((h) =>
    /Most (declared|depended-on) packages/.test(h.textContent ?? ''),
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
});
