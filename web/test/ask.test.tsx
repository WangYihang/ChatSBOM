/**
 * The natural-language seam.
 *
 * A generic UI is coming from elsewhere, so what matters here is the
 * boundary rather than the placeholder's looks: what a UI receives, what
 * it must never receive, and that progress and failure both arrive in a
 * form it can render.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { AskPlaceholder } from '../src/ask/Placeholder';
import type { AskProgress } from '../src/ask/contract';
import { QueryView } from '../src/components/QueryView';
import type { DatasetClient } from '../src/d1/client';
import { DICTIONARIES } from '../src/i18n/strings';
import { challenge, solving } from './altcha';

const EN = DICTIONARIES.en;

beforeEach(() => cleanup());

function submit(question: string) {
  fireEvent.change(screen.getByLabelText('Question'), {
    target: { value: question },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Ask' }));
}

describe('ask seam', () => {
  it('passes the question straight through', async () => {
    const ask = vi.fn().mockResolvedValue('42 projects.');
    render(<AskPlaceholder words={EN} ask={ask} />);
    submit('who declares mail?');
    await waitFor(() => expect(ask).toHaveBeenCalled());
    expect(ask.mock.calls[0]![0]).toBe('who declares mail?');
  });

  it('renders the answer as text, never as markup', async () => {
    const ask = vi.fn().mockResolvedValue('<b>not bold</b>');
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('anything');
    await waitFor(() =>
      expect(container.querySelector('.answer')!.textContent).toBe(
        '<b>not bold</b>',
      ),
    );
    expect(container.querySelector('.answer b')).toBeNull();
  });

  it('surfaces a rejection as a failure, distinguishable from an answer', async () => {
    const ask = vi.fn().mockRejectedValue(new Error('Too many questions.'));
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('anything');
    await waitFor(() =>
      expect(container.querySelector('.answer.error')).not.toBeNull(),
    );
    expect(container.textContent).toContain('Too many questions.');
  });

  it('reports progress while a question runs', async () => {
    const ask = vi.fn((_q: string, progress?: AskProgress) => {
      progress?.onThinking?.('checking both ecosystems\nsecond line');
      progress?.onToolCall?.('dependents_of', { name: 'mail' });
      return Promise.resolve('done');
    });
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('anything');
    await waitFor(() =>
      expect(container.querySelector('.trace')!.textContent).toContain(
        'dependents_of',
      ),
    );
    // Only the first line of a reasoning summary: the trace is a
    // progress log, not a transcript.
    expect(container.querySelector('.trace')!.textContent).toContain(
      'checking both ecosystems',
    );
    expect(container.querySelector('.trace')!.textContent).not.toContain(
      'second line',
    );
  });

  it('refuses to run an empty question', () => {
    const ask = vi.fn();
    render(<AskPlaceholder words={EN} ask={ask} />);
    fireEvent.click(screen.getByRole('button', { name: 'Ask' }));
    expect(ask).not.toHaveBeenCalled();
  });

  it('will not run two questions at once', async () => {
    let release: (value: string) => void = () => {};
    const ask = vi.fn(() => new Promise<string>((r) => (release = r)));
    render(<AskPlaceholder words={EN} ask={ask} />);
    submit('first');
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Asking…' })).toBeTruthy(),
    );
    fireEvent.click(screen.getByRole('button', { name: 'Asking…' }));
    expect(ask).toHaveBeenCalledTimes(1);
    release('done');
  });

  it("offers the page suggestions, and fills the box when one is chosen", () => {
    render(
      <AskPlaceholder words={EN} ask={vi.fn()} suggestions={['Which projects declare mail?']} />,
    );
    fireEvent.click(screen.getByText('Which projects declare mail?'));
    expect(
      (screen.getByLabelText('Question') as HTMLInputElement).value,
    ).toBe('Which projects declare mail?');
  });

  it('offers a new conversation once there is one, and starting it clears the panel (#42)', async () => {
    /**
     * The Worker ended a conversation past 40 messages with "Start a new
     * one", and nothing on the page could: only a reload would. The
     * service takes the last three exchanges as text (#144), and a
     * reader may still want to start over.
     */
    const ask = vi.fn((_q: string, progress?: AskProgress) => {
      progress?.onToolCall?.('dependents_of', { name: 'mail' });
      return Promise.resolve('first answer');
    });
    const reset = vi.fn();
    const { container } = render(<AskPlaceholder words={EN} ask={ask} reset={reset} />);
    const fresh = () =>
      screen.getByRole('button', { name: EN.askNewConversation }) as HTMLButtonElement;
    // Nothing to forget yet.
    expect(fresh().disabled).toBe(true);

    submit('who declares mail?');
    await waitFor(() => expect(container.textContent).toContain('first answer'));
    expect(fresh().disabled).toBe(false);

    fireEvent.click(fresh());
    expect(reset).toHaveBeenCalledTimes(1);
    expect(container.textContent).not.toContain('first answer');
    expect(container.querySelector('.trace')!.textContent).toBe('');
    expect(fresh().disabled).toBe(true);
  });

  it('will not start a new conversation while a question is running', async () => {
    let release: (value: string) => void = () => {};
    const ask = vi
      .fn()
      .mockResolvedValueOnce('first answer')
      .mockImplementation(() => new Promise<string>((r) => (release = r)));
    const reset = vi.fn();
    const { container } = render(<AskPlaceholder words={EN} ask={ask} reset={reset} />);
    submit('one');
    await waitFor(() => expect(container.textContent).toContain('first answer'));
    submit('two');
    const fresh = screen.getByRole('button', { name: EN.askNewConversation });
    expect((fresh as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(fresh);
    expect(reset).not.toHaveBeenCalled();
    release('done');
  });

  it('shows the answer as it is written, and then the answer (#144)', async () => {
    let finish: (text: string) => void = () => {};
    const ask = vi.fn((_q: string, progress?: AskProgress) => {
      progress?.onText?.('Seventeen projects');
      return new Promise<string>((resolve) => (finish = resolve));
    });
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('anything');
    await waitFor(() =>
      expect(container.querySelector('.answer')?.textContent).toBe('Seventeen projects'),
    );
    // Not an answer yet: nothing to open, and no new conversation.
    expect(container.querySelector('.answer.error')).toBeNull();
    expect(screen.getByRole('button', { name: 'Asking…' })).toBeTruthy();
    finish('Seventeen projects declare mail.');
    await waitFor(() =>
      expect(container.querySelector('.answer')?.textContent).toBe(
        'Seventeen projects declare mail.',
      ),
    );
  });

  it('takes back what was written when the question fails after all', async () => {
    let fail: (error: Error) => void = () => {};
    const ask = vi.fn((_q: string, progress?: AskProgress) => {
      progress?.onText?.('Seventeen proj');
      return new Promise<string>((_resolve, reject) => (fail = reject));
    });
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('anything');
    await waitFor(() => expect(container.textContent).toContain('Seventeen proj'));
    fail(new Error('The model could not be reached.'));
    await waitFor(() => expect(container.querySelector('.answer.error')).not.toBeNull());
    expect(container.textContent).not.toContain('Seventeen proj');
  });

  it('takes a question no longer than the service does', () => {
    render(<AskPlaceholder words={EN} ask={vi.fn()} />);
    expect((screen.getByLabelText('Question') as HTMLInputElement).maxLength).toBe(4_000);
  });

  it('clears the previous answer when a new question starts', async () => {
    const ask = vi
      .fn()
      .mockResolvedValueOnce('first answer')
      .mockImplementation(() => new Promise<string>(() => {}));
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('one');
    await waitFor(() =>
      expect(container.textContent).toContain('first answer'),
    );
    submit('two');
    expect(container.textContent).not.toContain('first answer');
  });

  describe('the packages an answer looked up (#123)', () => {
    /**
     * The page hands the slot `onPackage`, so an answer that names a
     * package can send the reader to that package's view (`contract.ts`),
     * and the placeholder, whose work is to run every part of the seam,
     * dropped it. What an answer is about is what its question looked
     * up: each tool that looks up a package names it `name`.
     */
    const looking = (answer: Promise<string>) =>
      vi.fn((_q: string, progress?: AskProgress) => {
        progress?.onToolCall?.('ecosystems_for', { name: 'mail' });
        progress?.onToolCall?.('dependents_of', { name: 'mail', type: 'gem' });
        progress?.onToolCall?.('search_packages', { fragment: 'rai' });
        progress?.onToolCall?.('version_spread', { name: 'rails' });
        return answer;
      });

    it('offers each, once, and opens the one chosen', async () => {
      const onPackage = vi.fn();
      render(
        <AskPlaceholder words={EN} ask={looking(Promise.resolve('Both.'))} onPackage={onPackage} />,
      );
      submit('who declares mail and rails?');
      const rails = await screen.findByRole('button', { name: 'rails' });
      expect(screen.getAllByRole('button', { name: /^(mail|rails)$/ }).map((b) => b.textContent))
        .toEqual(['mail', 'rails']);
      fireEvent.click(rails);
      expect(onPackage).toHaveBeenCalledExactlyOnceWith('rails');
    });

    it('offers none where the page gives no way to open one', async () => {
      const { container } = render(
        <AskPlaceholder words={EN} ask={looking(Promise.resolve('Both.'))} />,
      );
      submit('who declares mail?');
      await waitFor(() => expect(container.textContent).toContain('Both.'));
      expect(screen.queryByRole('button', { name: 'mail' })).toBeNull();
    });

    it('offers none for a question that failed, and forgets them for the next', async () => {
      const ask = looking(Promise.reject(new Error('Too many questions.')));
      const { container } = render(
        <AskPlaceholder words={EN} ask={ask} onPackage={vi.fn()} />,
      );
      submit('who declares mail?');
      await waitFor(() => expect(container.querySelector('.answer.error')).not.toBeNull());
      expect(screen.queryByRole('button', { name: 'mail' })).toBeNull();

      ask.mockImplementationOnce(() => Promise.resolve('Nothing looked up.'));
      submit('and now?');
      await waitFor(() => expect(container.textContent).toContain('Nothing looked up.'));
      expect(screen.queryByRole('button', { name: 'mail' })).toBeNull();
    });
  });
});

describe('the page, from the Ask panel (#123)', () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('opens a package the answer looked up, and brings the reader to it', async () => {
    // The service's answer (#144): a tool call that looks up `rails`,
    // which it runs itself, and then the answer.
    await solving();
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string) =>
        url === '/api/ask/challenge'
          ? new Response(JSON.stringify(challenge()), {
            headers: { 'content-type': 'application/json' },
          })
          : new Response(
            [
              'event: tool\ndata: {"name":"version_spread","arguments":{"name":"rails"}}\n\n',
              'event: text\ndata: {"delta":"Most run 7.1."}\n\n',
              'event: done\ndata: {"turns":2}\n\n',
            ].join(''),
            { headers: { 'content-type': 'text/event-stream' } },
          ),
      ),
    );
    const answers: Record<string, unknown> = {
      countDependents: 0,
      versionSpread: { versions: [], constrained: 0, unversioned: 0 },
      dependencyTree: { root: 'mail', children: [], grandchildren: [] },
      edgeAmbiguity: null,
    };
    const dataset = new Proxy({}, {
      get: (_target, key: string) => () =>
        Promise.resolve(key in answers ? answers[key] : []),
    }) as DatasetClient;
    // The top of the page, where the view the reader is sent to begins.
    const scrollTo = vi.spyOn(window, 'scrollTo').mockImplementation(() => {});
    const go = vi.fn();
    render(
      <QueryView
        words={EN}
        locale="en"
        dataset={dataset}
        languages={[]}
        route={{ view: 'query', package: 'mail' }}
        go={go}
      />,
    );
    fireEvent.change(await screen.findByLabelText('Question'), {
      target: { value: 'which rails is in use?' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Ask' }));

    fireEvent.click(await screen.findByRole('button', { name: 'rails' }));
    expect(go).toHaveBeenLastCalledWith({ view: 'query', package: 'rails' });
    // The panel is the view's last: the view changes above it, and the
    // reader, still at the answer, saw nothing happen.
    expect(scrollTo).toHaveBeenCalledWith({ top: 0 });
  });
});
