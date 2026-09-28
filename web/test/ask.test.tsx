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
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AskPlaceholder } from '../src/ask/Placeholder';
import type { AskProgress } from '../src/ask/contract';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

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
     * The Worker ends a conversation past 40 messages with "Start a new
     * one", and nothing on the page could: only a reload would.
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

  it('says so in the trace when the model paused a long turn and carried on', async () => {
    const ask = vi.fn((_q: string, progress?: AskProgress) => {
      progress?.onPause?.();
      return Promise.resolve('done');
    });
    const { container } = render(<AskPlaceholder words={EN} ask={ask} />);
    submit('anything');
    await waitFor(() =>
      expect(container.querySelector('.trace')!.textContent).toContain(EN.askPaused),
    );
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
});
