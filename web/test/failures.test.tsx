/**
 * A failure, said in the reader's language (#43).
 *
 * The Worker, the agent loop and the Turnstile widget each write their
 * failures in English — for the log, and for the English page — and the
 * Chinese page showed them as they came. The Worker is not told the
 * reader's language, and has no need to be: each failure says what kind
 * it is — the status the Worker refused with, why the model stopped,
 * which step of the challenge failed — and the page says that kind in
 * its own words. The Worker's English stays beside the Chinese only
 * where it says something the status does not: which argument was
 * wrong, or which of the refusals that share a status this was.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { Agent } from '../src/agent';
import { App } from '../src/app';
import { AskPlaceholder } from '../src/ask/Placeholder';
import { turnstileSolver } from '../src/ask/turnstile';
import { QueryView } from '../src/components/QueryView';
import { DatasetClient } from '../src/d1/client';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

/** Any letter of the Latin alphabet: English, where Chinese was due. */
const LATIN = /[A-Za-z]/;

const json = (payload: unknown, status = 200) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });

/** `/api/q` refusing every question with `status` and the Worker's `error`. */
function refuseQueries(status: number, error: string) {
  vi.stubGlobal('fetch', vi.fn(async () => json({ error }, status)));
}

beforeEach(() => {
  cleanup();
  localStorage.clear();
  window.history.replaceState(null, '', '#/overview');
  vi.stubGlobal('matchMedia', (query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }));
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

/** The query view for `mail`, asking the Worker for real. */
function queryView(locale: 'en' | 'zh') {
  render(
    <QueryView
      words={DICTIONARIES[locale]}
      locale={locale}
      dataset={new DatasetClient()}
      languages={[]}
      route={{ view: 'query', package: 'mail' }}
      go={vi.fn()}
    />,
  );
}

const statusLine = () => document.getElementById('status')!.textContent ?? '';

describe('a question the dataset refused', () => {
  it.each([
    [429, 'Too many queries. Wait a moment.'],
    [500, 'The query could not be answered.'],
    [503, 'No database bound to this deployment.'],
  ])('says a %i in Chinese alone', async (status, sentence) => {
    // One refusal per status at /api/q, so the status says it all.
    refuseQueries(status, sentence);
    queryView('zh');
    await waitFor(() => expect(statusLine()).not.toBe(ZH.statusSearching('mail')));
    expect(statusLine()).not.toMatch(LATIN);
    expect(statusLine()).toBe(ZH.queryRefused(status, sentence));
    // The tree's panel, which asks separately, says the same.
    expect(document.body.textContent).not.toContain(sentence);
  });

  it('keeps the Worker’s sentence where only it says what was wrong', async () => {
    // A 400 is one of a dozen refusals, each naming the argument.
    refuseQueries(400, 'Unknown method: dependentsOf');
    queryView('zh');
    await waitFor(() => expect(statusLine()).toContain('Unknown method: dependentsOf'));
    expect(statusLine()).toMatch(/[一-鿿]/);
  });

  it('says a question that never reached the Worker in Chinese too', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => {
      throw new TypeError('Failed to fetch');
    }));
    queryView('zh');
    await waitFor(() => expect(statusLine()).not.toBe(ZH.statusSearching('mail')));
    expect(statusLine()).toMatch(/[一-鿿]/);
    expect(statusLine()).toBe(ZH.queryFailed('Failed to fetch'));
  });

  it('says it in English as the Worker wrote it', async () => {
    refuseQueries(429, 'Too many queries. Wait a moment.');
    queryView('en');
    await waitFor(() => expect(statusLine()).toBe('Too many queries. Wait a moment.'));
  });

  it('says the page could not start, in Chinese', async () => {
    // The provenance is the page's first question; its failure is the
    // one message the page shows in place of both views.
    localStorage.setItem('chatsbom:locale', 'zh');
    refuseQueries(503, 'No database bound to this deployment.');
    render(<App />);
    const failed = await waitFor(() => {
      const node = document.querySelector('.answer.error');
      expect(node).not.toBeNull();
      return node!;
    });
    expect(failed.textContent).not.toMatch(LATIN);
    expect(failed.textContent).toBe(ZH.queryRefused(503, 'No database bound to this deployment.'));
  });
});

describe('the dictionary’s refusals', () => {
  const REFUSALS: [number, string][] = [
    [400, 'Conversation too long: 41 messages, limit 40. Start a new one.'],
    [403, 'Human verification has expired, or was for another question. Ask again.'],
    [413, 'Request too large.'],
    [429, 'The daily budget for AI answers is used up. The dashboard itself still works.'],
    [500, 'Unexpected failure.'],
    [502, 'The model could not be reached. Try again shortly.'],
    [503, 'AI answers are not configured on this deployment.'],
  ];

  it('says each in English as the Worker wrote it', () => {
    for (const [status, sentence] of REFUSALS) {
      expect(EN.askRefused(status, sentence)).toBe(sentence);
      expect(EN.queryRefused(status, sentence)).toBe(sentence);
    }
  });

  it('says in Chinese alone what a status says alone', () => {
    // One sentence each at /api/chat: too large, the Worker's own
    // failure, the model out of reach.
    for (const [status, sentence] of REFUSALS.filter(([s]) => [413, 500, 502].includes(s))) {
      expect(ZH.askRefused(status, sentence)).not.toMatch(LATIN);
    }
  });

  it('keeps the English where a status covers several refusals', () => {
    // Too many questions or the day's budget spent share 429; not set
    // up, set up wrong and a moment's outage share 503; and so on.
    for (const [status, sentence] of REFUSALS.filter(([s]) => [400, 403, 429, 503].includes(s))) {
      const said = ZH.askRefused(status, sentence);
      expect(said).toContain(sentence);
      expect(said).toMatch(/[一-鿿]/);
    }
  });
});

describe('a question the model could not answer', () => {
  const USAGE = { input_tokens: 1, output_tokens: 1 };

  /** The Ask panel, asking a real agent of a Worker that answers `turns`. */
  function askPanel(locale: 'en' | 'zh', ...turns: Response[]) {
    const fetch = vi.fn();
    for (const turn of turns) fetch.mockResolvedValueOnce(turn);
    vi.stubGlobal('fetch', fetch);
    const agent = new Agent({} as DatasetClient);
    const { container } = render(
      <AskPlaceholder words={DICTIONARIES[locale]} ask={(question) => agent.ask(question)} />,
    );
    fireEvent.change(screen.getByLabelText(DICTIONARIES[locale].askQuestionLabel), {
      target: { value: 'who declares mail?' },
    });
    fireEvent.click(screen.getByRole('button', { name: DICTIONARIES[locale].askButton }));
    return container;
  }

  const failure = (container: HTMLElement) =>
    waitFor(() => {
      const node = container.querySelector('.answer.error');
      expect(node).not.toBeNull();
      return node!.textContent ?? '';
    });

  it.each([
    ['max_tokens', 'cut off'],
    ['refusal', 'declined'],
    ['model_context_window_exceeded', 'too long'],
  ])('says why the model stopped (%s) in Chinese', async (stop_reason, english) => {
    const container = askPanel(
      'zh',
      json({ id: 'm', stop_reason, usage: USAGE, content: [] }),
    );
    const said = await failure(container);
    expect(said).not.toMatch(LATIN);
    expect(said).not.toContain(english);
  });

  it('names a stop it does not know, in Chinese around its name', async () => {
    const container = askPanel(
      'zh',
      json({ id: 'm', stop_reason: 'something_new', usage: USAGE, content: [] }),
    );
    const said = await failure(container);
    expect(said).toContain('something_new');
    expect(said.replace('something_new', '')).not.toMatch(LATIN);
  });

  it('says a refused turn in Chinese, keeping the sentence only where it tells which', async () => {
    const budget = 'The daily budget for AI answers is used up. The dashboard itself still works.';
    const refused = askPanel('zh', json({ error: budget }, 429));
    const said = await failure(refused);
    expect(said).toMatch(/[一-鿿]/);
    expect(said).toBe(ZH.askRefused(429, budget));
    cleanup();

    const unreachable = askPanel(
      'zh',
      json({ error: 'The model could not be reached. Try again shortly.' }, 502),
    );
    expect(await failure(unreachable)).not.toMatch(LATIN);
  });

  it('says it in English as it was raised', async () => {
    const container = askPanel(
      'en',
      json({ id: 'm', stop_reason: 'max_tokens', usage: USAGE, content: [] }),
    );
    expect(await failure(container)).toMatch(/cut off at its length limit/);
  });

  it('says a failed human verification in Chinese', async () => {
    // The page has nowhere to draw the widget: one of the challenge's
    // own failures, raised in the page rather than by the Worker.
    const solve = turnstileSolver(() => null, () => 'zh');
    render(
      <AskPlaceholder words={ZH} ask={() => solve({ siteKey: 'the-site-key', action: 'ask' })} />,
    );
    fireEvent.change(screen.getByLabelText(ZH.askQuestionLabel), { target: { value: 'q' } });
    fireEvent.click(screen.getByRole('button', { name: ZH.askButton }));
    const said = await failure(document.body);
    expect(said).not.toMatch(LATIN);
  });

  it('says what it can of a failure it does not know, in Chinese around it', async () => {
    render(
      <AskPlaceholder words={ZH} ask={() => Promise.reject(new Error('Something odd.'))} />,
    );
    fireEvent.change(screen.getByLabelText(ZH.askQuestionLabel), { target: { value: 'q' } });
    fireEvent.click(screen.getByRole('button', { name: ZH.askButton }));
    const said = await failure(document.body);
    expect(said).toContain('Something odd.');
    expect(said).toMatch(/[一-鿿]/);
  });
});
