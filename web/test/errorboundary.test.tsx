/**
 * What the page shows when drawing it fails.
 *
 * Nothing caught a render error, so one left the page blank — and when
 * the cause was in the address, blank on every reload too (#42). The
 * boundary is the second line of defence behind fixing each cause: a
 * page that cannot be drawn says so, and offers the way back.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

import { ErrorBoundary } from '../src/components/ErrorBoundary';
import { DICTIONARIES } from '../src/i18n/strings';

beforeEach(() => {
  cleanup();
  localStorage.clear();
  window.history.replaceState(null, '', '#/overview');
  // React reports a caught render error on the console as well; the
  // boundary is the subject here, not the log.
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

/** A view that cannot be drawn at one address. */
function Fragile() {
  if (window.location.hash === '#/broken') throw new Error('cannot draw this');
  return <p>drawn</p>;
}

it('says the page could not be drawn, rather than drawing nothing', () => {
  window.history.replaceState(null, '', '#/broken');
  render(
    <ErrorBoundary>
      <Fragile />
    </ErrorBoundary>,
  );
  expect(screen.getByRole('alert').textContent).toMatch(/could not be drawn/i);
});

it('says so in the language the reader chose (#43)', () => {
  // It sits above the language switch, and spoke English to everyone
  // for that reason. The choice is stored, and the switch starts from
  // it; so can this.
  localStorage.setItem('chatsbom:locale', 'zh');
  window.history.replaceState(null, '', '#/broken');
  render(
    <ErrorBoundary>
      <Fragile />
    </ErrorBoundary>,
  );
  expect(screen.getByRole('alert').textContent).toContain(DICTIONARIES.zh.boundaryFailed);
  expect(screen.getByRole('link', { name: DICTIONARIES.zh.boundaryBack })).toBeTruthy();
});

it('goes back to the overview, and draws the page again', () => {
  window.history.replaceState(null, '', '#/broken');
  render(
    <ErrorBoundary>
      <Fragile />
    </ErrorBoundary>,
  );
  fireEvent.click(screen.getByRole('link', { name: /overview/i }));
  expect(window.location.hash).toBe('#/overview');
  expect(screen.getByText('drawn')).toBeTruthy();
});
