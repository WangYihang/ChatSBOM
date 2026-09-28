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
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RankedBars } from '../src/charts/RankedBars';
import { SEQUENTIAL_DARK, SEQUENTIAL_LIGHT } from '../src/palette';
import { useTheme } from '../src/theme';

let listeners: (() => void)[] = [];
let dark = false;

beforeEach(() => {
  cleanup();
  listeners = [];
  dark = false;
  localStorage.clear();
  document.documentElement.removeAttribute('data-theme');
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

  it('draws in the palette chosen on the first toggle, not the one before it (#42)', () => {
    /**
     * The choice reached the document in an effect, which runs after
     * the render it caused — and the charts read the document while
     * rendering. So a click on Dark redrew every chart in the light
     * palette, and the next click drew the one before it.
     */
    function Page() {
      const { setChoice } = useTheme();
      return (
        <>
          <button type="button" onClick={() => setChoice('dark')}>dark</button>
          <button type="button" onClick={() => setChoice('light')}>light</button>
          <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" />
        </>
      );
    }
    const { container } = render(<Page />);
    expect(SEQUENTIAL_LIGHT).toContain(fill(container));

    fireEvent.click(screen.getByText('dark'));
    expect(SEQUENTIAL_DARK).toContain(fill(container));

    fireEvent.click(screen.getByText('light'));
    expect(SEQUENTIAL_LIGHT).toContain(fill(container));
  });

  it('draws a stored choice from the first render', () => {
    // Read before anything is drawn, so no chart starts in the system
    // palette and waits for something else to redraw it.
    localStorage.setItem('chatsbom:theme', 'dark');
    function Page() {
      useTheme();
      return <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" />;
    }
    const { container } = render(<Page />);
    expect(SEQUENTIAL_DARK).toContain(fill(container));
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
