/**
 * What the Ask panel tells the reader about where their question goes.
 *
 * It said "the data never leaves your browser". The agent loop does run
 * in the page, but every turn is posted to `/api/chat` and forwarded to
 * Anthropic, and the tool results come back as the next user message —
 * so the rows the model reasons over are exactly what is sent. A
 * privacy claim is the one kind of copy that has to be right, and a
 * reader who believed this one would have been misled about their own
 * data.
 */
// @vitest-environment jsdom
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe as group, expect, it, vi } from 'vitest';
import { QueryView } from '../src/components/QueryView';
import type { DatasetClient } from '../src/d1/client';

beforeEach(() => cleanup());

/**
 * Answers shaped the way each method really answers. An empty array
 * where an object is expected reads as "undefined.length" three panels
 * down, which is a fault in the fixture rather than in the page.
 */
const ANSWERS: Record<string, unknown> = {
  dependentsOf: [],
  countDependents: 0,
  ecosystemsFor: [],
  versionSpread: { resolved: [], constraints: [] },
  adoptionOverTime: [],
  dependencyTree: { root: 'mail', children: [] },
  pulledInBy: [],
  edgeAmbiguity: null,
};

const client = (): DatasetClient =>
  new Proxy({}, {
    get(_t, key: string) {
      return () =>
        key in ANSWERS
          ? Promise.resolve(ANSWERS[key])
          : Promise.resolve([]);
    },
  }) as DatasetClient;

const panel = (): string => {
  const heading = [...document.querySelectorAll('h2, h3')].find((h) =>
    /Ask a question/.test(h.textContent ?? ''),
  );
  return heading?.parentElement?.textContent ?? '';
};

group('Ask a question', () => {
  const mount = () =>
    render(
      <QueryView
        dataset={client()}
        languages={['php']}
        route={{ view: 'query', package: 'mail' }}
        go={vi.fn()}
      />,
    );

  it('does not claim the data stays in the browser', async () => {
    mount();
    await waitFor(() => expect(screen.getByText(/Ask a question/)).toBeTruthy());
    expect(panel()).not.toContain('never leaves your browser');
  });

  it('says where the question and the rows actually go', async () => {
    mount();
    await waitFor(() => expect(screen.getByText(/Ask a question/)).toBeTruthy());
    expect(panel()).toContain('sent to Anthropic');
  });

  it('keeps the claim that is true — no SQL, no database access', async () => {
    mount();
    await waitFor(() => expect(screen.getByText(/Ask a question/)).toBeTruthy());
    expect(panel()).toMatch(/cannot write SQL/);
    expect(panel()).toMatch(/reach the database/);
  });
});
