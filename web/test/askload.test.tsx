/**
 * The widget, loaded when the first question is asked (#144), and asked
 * for again when that load failed.
 *
 * It is a file of its own, fetched then: over a connection that drops,
 * it can fail to come. The question is then said to have failed its
 * check, in the reader's words, and the next question loads it again,
 * rather than failing for good until a reload.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';

import { AskSlot } from '../src/ask/Slot';
import { DICTIONARIES } from '../src/i18n/strings';
import { challenge, solving } from './altcha';

const ZH = DICTIONARIES.zh;

/** Whether the widget's module fails to load, and how often it was asked for. */
const loading = vi.hoisted(() => ({ fail: true, asked: 0 }));

vi.mock('../src/ask/altcha', async (original) => {
  loading.asked += 1;
  if (loading.fail) throw new TypeError('Failed to fetch dynamically imported module');
  return original();
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it('says a widget that did not load, and loads it for the next question', async () => {
  vi.stubGlobal('fetch', async (url: string) =>
    url === '/api/ask/challenge'
      ? new Response(JSON.stringify(challenge()), {
        headers: { 'content-type': 'application/json' },
      })
      : new Response('event: text\ndata: {"delta":"四个。"}\n\nevent: done\ndata: {}\n\n', {
        headers: { 'content-type': 'text/event-stream' },
      }),
  );
  render(<AskSlot words={ZH} onPackage={vi.fn()} suggestions={[]} />);
  const ask = (question: string) => {
    fireEvent.change(screen.getByLabelText(ZH.askQuestionLabel), { target: { value: question } });
    fireEvent.click(screen.getByRole('button', { name: ZH.askButton }));
  };
  // Not with the panel: with the question.
  expect(loading.asked).toBe(0);

  ask('谁声明了 mail？');
  await waitFor(() =>
    expect(document.querySelector('.answer.error')?.textContent).toBe(
      ZH.askUnanswered({ code: 'unverified', said: '', status: null }),
    ),
  );
  expect(loading.asked).toBe(1);

  loading.fail = false;
  await solving();
  ask('谁声明了 mail？');
  await screen.findByText('四个。');
  expect(loading.asked).toBeGreaterThanOrEqual(2);
});
