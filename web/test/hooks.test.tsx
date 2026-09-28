/**
 * `useAsync`: what a superseded question costs, and what the page shows
 * while the next one loads (#42).
 */
// @vitest-environment jsdom
import { cleanup, renderHook, waitFor } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import { useAsync } from '../src/hooks';

afterEach(() => cleanup());

/** A question whose answer is given by hand, with the signal it was asked with. */
function pending() {
  const runs: { question: string; signal: AbortSignal | undefined; answer(value: string): void }[] = [];
  const run = (question: string) => (signal?: AbortSignal) =>
    new Promise<string>((resolve) => {
      runs.push({ question, signal, answer: resolve });
    });
  return { runs, run };
}

describe('useAsync', () => {
  it('aborts the question it no longer wants', async () => {
    // The stale answer was discarded, but its request ran to the end:
    // each keystroke's query still cost the store a query.
    const { runs, run } = pending();
    const { rerender } = renderHook(
      ({ question }) => useAsync(run(question), [question]),
      { initialProps: { question: 'ma' } },
    );
    rerender({ question: 'mail' });

    expect(runs.map(({ question }) => question)).toEqual(['ma', 'mail']);
    expect(runs[0]!.signal?.aborted).toBe(true);
    expect(runs[1]!.signal?.aborted).toBe(false);
  });

  it('aborts on unmount', () => {
    const { runs, run } = pending();
    const { unmount } = renderHook(() => useAsync(run('mail'), []));
    unmount();
    expect(runs[0]!.signal?.aborted).toBe(true);
  });

  it('keeps the last answer while the next one loads', async () => {
    // So a view can go on showing it rather than emptying, and jumping,
    // for the length of a round trip.
    const { runs, run } = pending();
    const { result, rerender } = renderHook(
      ({ page }) => useAsync(run(`page ${page}`), [page]),
      { initialProps: { page: 1 } },
    );
    runs[0]!.answer('rows of page 1');
    await waitFor(() => expect(result.current).toEqual({
      status: 'ready', value: 'rows of page 1',
    }));

    rerender({ page: 2 });
    expect(result.current).toEqual({ status: 'loading', previous: 'rows of page 1' });

    runs[1]!.answer('rows of page 2');
    await waitFor(() => expect(result.current).toEqual({
      status: 'ready', value: 'rows of page 2',
    }));
  });

  it('never publishes an answer to a question it has moved on from', async () => {
    const { runs, run } = pending();
    const { result, rerender } = renderHook(
      ({ question }) => useAsync(run(question), [question]),
      { initialProps: { question: 'ma' } },
    );
    rerender({ question: 'mail' });
    runs[1]!.answer('mail');
    runs[0]!.answer('ma');
    await waitFor(() => expect(result.current).toEqual({ status: 'ready', value: 'mail' }));
  });
});
