/**
 * The proof of work a question carries, through the Ask panel (#144).
 *
 * ALTCHA's widget replaces Turnstile's (#128 §2.8). Before each question
 * it asks the service for a challenge, `GET /api/ask/challenge`, solves
 * it in Web Workers, and its solution goes with the question, `POST
 * /api/ask`, which verifies it once (`chatsbom/server/challenge.py`).
 * This runs the panel as the page mounts it, with the page's own widget
 * (`altcha/external`, `src/ask/altcha.ts`), the network stood in for,
 * and the workers too, which jsdom does not run (`altcha.ts`).
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryView } from '../src/components/QueryView';
import type { DatasetClient } from '../src/dataset/client';
import { DICTIONARIES } from '../src/i18n/strings';
import { challenge, payload, solving } from './altcha';
import { WHOLE_PAGE } from './answers';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

/** A dataset with nothing in it, answering in the shapes the page expects. */
const ANSWERS: Record<string, unknown> = {
  countDependents: 0,
  versionSpread: { versions: [], constrained: 0, unversioned: 0 },
  dependencyTree: { root: 'mail', children: [], grandchildren: [] },
  edgeAmbiguity: null,
};

const dataset = (): DatasetClient =>
  new Proxy(
    {},
    {
      get: (_target, key: string) => () =>
        Promise.resolve(key in ANSWERS ? ANSWERS[key] : []),
    },
  ) as DatasetClient;

const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { 'content-type': 'application/json' },
  });

const answered = (text: string) =>
  new Response(
    `event: text\ndata: ${JSON.stringify({ delta: text })}\n\n`
      + 'event: done\ndata: {"turns":1}\n\n',
    { headers: { 'content-type': 'text/event-stream' } },
  );

/** One request the page made of the service. */
interface Sent {
  method: string;
  url: string;
  body: unknown;
}

/**
 * The service: a challenge from `issue`, a question answered by
 * `answer`, each request kept in the order it came.
 */
function stubService(
  issue: () => Response = () => json(challenge()),
  answer: () => Response = () => answered('Seventeen.'),
) {
  const sent: Sent[] = [];
  const issued: unknown[] = [];
  vi.stubGlobal('fetch', async (url: string, init?: RequestInit) => {
    const method = init?.method ?? 'GET';
    sent.push({ method, url, body: init?.body ? JSON.parse(String(init.body)) : null });
    if (url === '/api/ask/challenge') {
      const response = issue();
      issued.push(await response.clone().json().catch(() => null));
      return response;
    }
    if (url === '/api/ask' && method === 'POST') return answer();
    throw new Error(`the page asked ${method} ${url}`);
  });
  return { sent, issued };
}

function mount(locale: 'en' | 'zh' = 'en') {
  render(
    <QueryView
      words={DICTIONARIES[locale]}
      locale={locale}
      dataset={dataset()}
      languages={[]}
      route={{ view: 'query', package: 'mail' }}
      go={vi.fn()}
    />,
  );
}

async function submit(question: string, locale: 'en' | 'zh' = 'en') {
  const words = DICTIONARIES[locale];
  fireEvent.change(await screen.findByLabelText(words.askQuestionLabel, undefined, WHOLE_PAGE), {
    target: { value: question },
  });
  fireEvent.click(screen.getByRole('button', { name: words.askButton }));
}

/** What the panel says when the question failed. */
const failure = () =>
  waitFor(() => {
    const node = document.querySelector('.answer.error');
    expect(node).not.toBeNull();
    return node!.textContent ?? '';
  }, WHOLE_PAGE);

beforeEach(() => cleanup());
// Unmounted after the test as well as before the next: the view's
// debounce timer otherwise fires after jsdom is torn down.
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe('a question', () => {
  it('fetches a challenge, solves it, and sends its solution with the question', async () => {
    const workers = await solving();
    const { sent, issued } = stubService();
    mount();
    await submit('who declares mail?');

    await screen.findByText('Seventeen.', undefined, WHOLE_PAGE);
    // The challenge first, then the question with what solves it.
    expect(sent.map(({ method, url }) => `${method} ${url}`)).toEqual([
      'GET /api/ask/challenge',
      'POST /api/ask',
    ]);
    expect(sent[1]!.body).toEqual({
      question: 'who declares mail?',
      prior: [],
      altcha: payload(issued[0] as ReturnType<typeof challenge>),
    });
    // Solved in workers, which are stopped once it is.
    expect(workers.length).toBeGreaterThan(0);
    expect(workers.every((worker) => worker.terminated)).toBe(true);
    // And the widget taken down with them.
    expect(document.querySelector('altcha-widget')).toBeNull();
  });

  it('solves a challenge of its own each time, and brings the earlier exchange', async () => {
    await solving();
    const { sent, issued } = stubService();
    mount();
    await submit('who declares mail?');
    await screen.findByText('Seventeen.', undefined, WHOLE_PAGE);
    await submit('and rails?');
    await waitFor(() => expect(sent).toHaveLength(4), WHOLE_PAGE);

    expect(sent.map(({ method, url }) => `${method} ${url}`)).toEqual([
      'GET /api/ask/challenge',
      'POST /api/ask',
      'GET /api/ask/challenge',
      'POST /api/ask',
    ]);
    expect(sent[3]!.body).toEqual({
      question: 'and rails?',
      prior: [{ q: 'who declares mail?', a: 'Seventeen.' }],
      altcha: payload(issued[1] as ReturnType<typeof challenge>),
    });
  });

  it('draws the widget in the Ask panel, out of sight, while it solves', async () => {
    await solving('hold');
    stubService();
    mount();
    await submit('who declares mail?');

    const widget = await waitFor(() => {
      const found = document.querySelector('altcha-widget');
      expect(found?.querySelector('.altcha')).toBeTruthy();
      return found!;
    }, WHOLE_PAGE);
    const panel = screen.getByLabelText(EN.askQuestionLabel).closest('section, .rail');
    expect(panel?.contains(widget)).toBe(true);
    // No checkbox for a reader to click: the page solves it for them.
    expect(widget.querySelector('.altcha')!.getAttribute('data-display')).toBe('invisible');
  });
});

describe('a challenge that cannot be had', () => {
  it.each([
    [503, 'AI answers are not configured on this deployment.', 'off'],
    [429, 'Too many questions. Wait a moment.', 'rate'],
  ])('says why the service refused one (%i), in either language', async (status, said, code) => {
    await solving();
    for (const locale of ['en', 'zh'] as const) {
      const { sent } = stubService(() => json({ error: said, code }, status));
      mount(locale);
      await submit('who declares mail?', locale);
      expect(await failure()).toBe(
        DICTIONARIES[locale].askUnanswered({ code, said, status }),
      );
      // No question is sent without its challenge.
      expect(sent.map(({ url }) => url)).toEqual(['/api/ask/challenge']);
      cleanup();
    }
    expect(EN.askUnanswered({ code: 'off', said: '', status: 503 })).not.toBe(
      ZH.askUnanswered({ code: 'off', said: '', status: 503 }),
    );
  });

  it('says the check could not be done when its workers fail', async () => {
    await solving('fail');
    const { sent } = stubService();
    mount('zh');
    await submit('谁声明了 mail？', 'zh');
    expect(await failure()).toBe(
      ZH.askUnanswered({ code: 'unverified', said: '', status: null }),
    );
    expect(sent.map(({ url }) => url)).toEqual(['/api/ask/challenge']);
    expect(document.querySelector('altcha-widget')).toBeNull();
  });
});
