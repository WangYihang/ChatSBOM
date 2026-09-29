/**
 * The Ask panel's code, loaded when the view that draws it is first
 * shown (#123).
 *
 * The page splits the panel's agent loop, its tools and the challenge
 * into a chunk of their own, loaded when the panel is first drawn (#44,
 * `split.test.ts`). But both views stay mounted, the one not shown kept
 * hidden, so the query view drew the panel as the page started, and the
 * chunk came right after the page's own, for every visitor to the
 * overview, whether or not they went on to ask anything.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

import { App } from '../src/app';
import { DICTIONARIES } from '../src/i18n/strings';
import { stubQueries, WHOLE_PAGE } from './answers';

const EN = DICTIONARIES.en;

/** How many times the page has loaded the Ask panel's module. */
const loads = vi.hoisted(() => ({ ask: 0 }));

vi.mock('../src/ask/Slot', async (original) => {
  loads.ask += 1;
  return original();
});

beforeEach(() => {
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

it('loads the Ask panel when the query view is first shown, not with the overview', async () => {
  stubQueries();
  render(<App />);
  // The overview drawn from its answers, the query view mounted, hidden,
  // beside it.
  await waitFor(
    () => expect(document.querySelector('g[data-row="serde"]')).not.toBeNull(),
    WHOLE_PAGE,
  );
  expect(document.querySelector('section[hidden] #status')).not.toBeNull();
  expect(loads.ask).toBe(0);
  expect(document.getElementById('question')).toBeNull();

  fireEvent.click(screen.getByRole('button', { name: EN.viewQuery }));
  const question = await screen.findByLabelText(EN.askQuestionLabel, undefined, WHOLE_PAGE);
  expect(loads.ask).toBe(1);

  // Kept once drawn: a question half-typed is where it was left after a
  // visit to the overview, and the panel is not loaded again.
  fireEvent.change(question, { target: { value: 'who declares mail?' } });
  fireEvent.click(screen.getByRole('button', { name: EN.viewOverview }));
  fireEvent.click(screen.getByRole('button', { name: EN.viewQuery }));
  expect((screen.getByLabelText(EN.askQuestionLabel) as HTMLInputElement).value).toBe(
    'who declares mail?',
  );
  expect(loads.ask).toBe(1);
}, WHOLE_PAGE.timeout * 3);

it('loads it with the page when the page opens on the query view', async () => {
  stubQueries();
  window.history.replaceState(null, '', '#/query/mail');
  render(<App />);
  await screen.findByLabelText(EN.askQuestionLabel, undefined, WHOLE_PAGE);
}, WHOLE_PAGE.timeout * 3);
