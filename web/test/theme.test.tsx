/**
 * Charts must follow a theme change.
 *
 * The palette is read at render time rather than cached, because a
 * cached one leaves a panel in the other theme's colours after a switch.
 * Reading at render time is only half the requirement though: React has
 * to be told to render again, and nothing in the DOM changes when the OS
 * theme flips. This pins the subscription.
 */
// @vitest-environment jsdom
import { act, cleanup, render } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RankedBars } from '../src/charts/RankedBars';

let listeners: (() => void)[] = [];
let dark = false;

beforeEach(() => {
  cleanup();
  listeners = [];
  dark = false;
  vi.stubGlobal(
    'matchMedia',
    (query: string) => ({
      matches: dark && query.includes('dark'),
      media: query,
      addEventListener: (_: string, fn: () => void) => listeners.push(fn),
      removeEventListener: (_: string, fn: () => void) => {
        listeners = listeners.filter((l) => l !== fn);
      },
    }),
  );
});

const fill = (host: HTMLElement) =>
  host.querySelector('path')!.getAttribute('fill');

describe('chart theming', () => {
  it('redraws in the other palette when the OS theme changes', () => {
    const { container } = render(
      <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" />,
    );
    const light = fill(container);

    act(() => {
      dark = true;
      for (const notify of listeners) notify();
    });

    expect(fill(container)).not.toBe(light);
  });

  it('subscribes once and unsubscribes on unmount', () => {
    const { unmount } = render(
      <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" />,
    );
    expect(listeners.length).toBeGreaterThan(0);
    unmount();
    expect(listeners).toHaveLength(0);
  });

  it('renders without matchMedia at all', () => {
    // Some environments have no matchMedia; a chart that throws while
    // drawing leaves a blank panel rather than a degraded one.
    vi.stubGlobal('matchMedia', undefined);
    const { container } = render(
      <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" />,
    );
    expect(container.querySelectorAll('path')).toHaveLength(1);
  });
});
