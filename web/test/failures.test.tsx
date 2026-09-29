/**
 * A failure, said in the reader's language (#43).
 *
 * The service writes its failures in English — for the log, and for the
 * English page — and the Chinese page showed them as they came. The
 * service is not told the reader's language, and has no need to be:
 * each failure says what kind it is — the status a query was refused
 * with, the code a question failed with (#144) — and the page says that
 * kind in its own words. The service's English stays beside the Chinese
 * only where it says something the kind does not: which argument was
 * wrong, or which of the refusals that share a status this was.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { App } from '../src/app';
import { AskPlaceholder } from '../src/ask/Placeholder';
import { ask } from '../src/ask/stream';
import { Overview } from '../src/components/Overview';
import { QueryView } from '../src/components/QueryView';
import { DatasetClient } from '../src/d1/client';
import { DICTIONARIES } from '../src/i18n/strings';
import { ANSWERS, stubQueries } from './answers';

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
    // The rate limit's own (#115): set wrong, or not countable for a moment.
    [503, 'The query endpoint is not set up correctly on this deployment.'],
    [503, 'Queries cannot be counted for a moment. Try again shortly.'],
    // The Python service's (#144): no dataset, or none readable for a
    // moment; and a snapshot gone, still gone once `meta` was asked again.
    [503, 'No dataset is configured on this deployment.'],
    [503, 'The dataset cannot be read for a moment. Try again shortly.'],
    [410, 'This snapshot of the dataset is no longer served. Reload the page.'],
  ])('says a %i in Chinese alone', async (status, sentence) => {
    // The status says it in Chinese, so what it says is true of each
    // of /api/q's refusals with that status.
    refuseQueries(status, sentence);
    queryView('zh');
    await waitFor(() => expect(statusLine()).not.toBe(ZH.statusSearching('mail')));
    expect(statusLine()).not.toMatch(LATIN);
    expect(statusLine()).toBe(ZH.queryRefused(status, sentence));
    // The tree's panel, which asks separately, says the same.
    expect(document.body.textContent).not.toContain(sentence);
  });

  it('says a 503 as a deployment that cannot answer, which each of them is (#115)', () => {
    // A 503 was one refusal, a deployment with no database, and the
    // Chinese said so. The rate limit has two more: a limit set wrong,
    // and a limiter out for a moment, neither of them about a database.
    expect(ZH.queryRefused(503, 'Queries cannot be counted for a moment. Try again shortly.')).not.toContain(
      '数据库',
    );
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

describe('a panel whose question failed (#123)', () => {
  /** `/api/q`, refusing the methods in `refused` as the Worker does and answering the rest. */
  const refuseSome = (refused: Record<string, readonly [number, string]>) =>
    stubQueries(ANSWERS, refused);

  /** Every method there is an answer for, refused with `status` and `error`. */
  const refuseAll = (status: number, error: string) =>
    Object.fromEntries(Object.keys(ANSWERS).map((method) => [method, [status, error] as const]));

  /** The panel headed `title`: the heading and everything under it. */
  function panel(title: string): HTMLElement {
    const heading = [...document.querySelectorAll('.panel h2')].find((h) =>
      (h.textContent ?? '').startsWith(title),
    );
    if (!heading) throw new Error(`no panel headed ${title}`);
    return heading.closest('.panel') as HTMLElement;
  }

  const ANSWERED = 'The query could not be answered.';
  const TOO_MANY = 'Too many queries. Wait a moment.';

  it.each(['en', 'zh'] as const)(
    'says on the overview that its question failed, not that it found nothing (%s)',
    async (locale) => {
      // The licences are refused as a failing store is, the histogram's
      // buckets as a busy client is, and the rest are answered.
      const words = DICTIONARIES[locale];
      refuseSome({ licenseShares: [500, ANSWERED], dependencyDistribution: [429, TOO_MANY] });
      render(
        <Overview
          words={words}
          locale={locale}
          dataset={new DatasetClient()}
          ecosystems={['npm']}
          go={vi.fn()}
        />,
      );
      await waitFor(() => expect(document.querySelector('g[data-row="serde"]')).not.toBeNull());

      await waitFor(() =>
        expect(panel(words.licencesTitle).textContent).toContain(words.queryRefused(500, ANSWERED)),
      );
      expect(panel(words.licencesTitle).textContent).not.toContain(words.noDataForSelection);
      expect(panel(words.bucketsTitle).textContent).toContain(words.queryRefused(429, TOO_MANY));
      expect(panel(words.bucketsTitle).textContent).not.toContain(words.noDataForSelection);
      // Said as a failure, not in the words or the look of an empty one.
      expect(panel(words.licencesTitle).querySelector('.error')).not.toBeNull();
      // The panels whose questions were answered draw them.
      expect(panel(words.coverageTitle).querySelector('g[data-row="rust"]')).not.toBeNull();
    },
  );

  it('says so in every panel of the overview when every question fails', async () => {
    refuseSome(refuseAll(500, ANSWERED));
    render(
      <Overview words={ZH} locale="zh" dataset={new DatasetClient()} ecosystems={[]} go={vi.fn()} />,
    );
    // The share bar under the page's claim, and the seven panels.
    await waitFor(() => expect(document.querySelectorAll('.error')).toHaveLength(8));
    for (const failed of document.querySelectorAll('.error')) {
      expect(failed.textContent).toBe(ZH.queryRefused(500, ANSWERED));
    }
    expect(document.body.textContent).not.toContain(ZH.noDataForSelection);
  });

  it('says so on the query view: its versions, its adoption, and its edges', async () => {
    refuseSome({
      versionSpread: [500, ANSWERED],
      adoptionOverTime: [500, ANSWERED],
      pulledInBy: [429, TOO_MANY],
      dependencyTree: [500, ANSWERED],
    });
    queryView('en');

    // The versions and the adoption share a panel, a heading each. It is
    // drawn once the table has rows.
    await waitFor(() =>
      expect(panel(EN.versionsTitle).querySelectorAll('.error')).toHaveLength(2),
    );
    for (const failed of panel(EN.versionsTitle).querySelectorAll('.error')) {
      expect(failed.textContent).toBe(ANSWERED);
    }
    expect(panel(EN.versionsTitle).textContent).not.toContain(EN.noDataForSelection);
    expect(panel(EN.versionsTitle).textContent).not.toContain(EN.adoptionEmpty);
    await waitFor(() =>
      expect(panel(EN.pulledInTitle).querySelector('.error')?.textContent).toBe(TOO_MANY),
    );
    expect(panel(EN.pulledInTitle).textContent).not.toContain(EN.noDataForSelection);
    // The tree said its failure already, and now looks it as the others do.
    await waitFor(() =>
      expect(panel(EN.pullsInTitle).querySelector('.error')?.textContent).toBe(ANSWERED),
    );
  });
});

describe('a question the service did not answer (#144)', () => {
  /** The service answering every question with `reply`. */
  function askPanel(locale: 'en' | 'zh', reply: () => Response) {
    vi.stubGlobal('fetch', vi.fn(async () => reply()));
    const { container } = render(
      <AskPlaceholder
        words={DICTIONARIES[locale]}
        ask={(question, progress) => ask({ question, prior: [], altcha: 'c29sdmVk' }, progress)}
      />,
    );
    fireEvent.change(screen.getByLabelText(DICTIONARIES[locale].askQuestionLabel), {
      target: { value: 'who declares mail?' },
    });
    fireEvent.click(screen.getByRole('button', { name: DICTIONARIES[locale].askButton }));
    return container;
  }

  const stopped = (data: Record<string, unknown>) => () =>
    new Response(`event: error\ndata: ${JSON.stringify({ turns: 1, ...data })}\n\n`, {
      headers: { 'content-type': 'text/event-stream' },
    });

  const failure = (container: HTMLElement) =>
    waitFor(() => {
      const node = container.querySelector('.answer.error');
      expect(node).not.toBeNull();
      return node!.textContent ?? '';
    });

  it.each([
    ['cut-off', 'The answer was cut off at its length limit before it finished. Try a narrower question.'],
    ['declined', 'The model declined to answer this question.'],
    ['timeout', 'The model took too long to answer. Try again shortly.'],
    ['garbled', "The model's answer was not understood."],
  ])('says why the model stopped (%s) in Chinese', async (code, message) => {
    const said = await failure(askPanel('zh', stopped({ code, message })));
    expect(said).not.toMatch(LATIN);
    expect(said).toBe(ZH.askUnanswered({ code, said: message, status: null, turns: 1 }));
  });

  it('names a stop it does not know, in Chinese around its name', async () => {
    const said = await failure(
      askPanel(
        'zh',
        stopped({
          code: 'stopped',
          message: 'The model stopped without an answer (something_new).',
          reason: 'something_new',
        }),
      ),
    );
    expect(said).toContain('something_new');
    expect(said.replace('something_new', '')).not.toMatch(LATIN);
  });

  it('says a refused question in Chinese, by its code', async () => {
    const budget = 'The daily budget for AI answers is used up. The dashboard itself still works.';
    const refused = askPanel('zh', () => json({ error: budget, code: 'budget' }, 429));
    const said = await failure(refused);
    expect(said).not.toMatch(LATIN);
    expect(said).toBe(ZH.askUnanswered({ code: 'budget', said: budget, status: 429 }));
    cleanup();

    const unreachable = askPanel(
      'zh',
      stopped({ code: 'model', message: 'The model could not be reached. Try again shortly.' }),
    );
    expect(await failure(unreachable)).not.toMatch(LATIN);
  });

  it('keeps the English only where it says what was wrong', async () => {
    const tooLong = 'Question too long: 4001 characters, limit 4000.';
    const said = await failure(askPanel('zh', () => json({ error: tooLong, code: 'invalid' }, 400)));
    expect(said).toContain(tooLong);
    expect(said).toMatch(/[一-鿿]/);
  });

  it('says it in English in its own words', async () => {
    const container = askPanel(
      'en',
      stopped({ code: 'cut-off', message: 'Cut off.' }),
    );
    expect(await failure(container)).toMatch(/cut off at its length limit/);
  });

  it('says in Chinese an answer that stopped arriving', async () => {
    const said = await failure(
      askPanel('zh', () =>
        new Response('event: text\ndata: {"delta":"Four"}\n\n', {
          headers: { 'content-type': 'text/event-stream' },
        }),
      ),
    );
    expect(said).not.toMatch(LATIN);
    expect(said).toBe(ZH.askUnanswered({ code: 'interrupted', said: '', status: null }));
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
