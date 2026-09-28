/**
 * The page, asking a question of a deployment that requires Turnstile
 * (#32).
 *
 * Nothing on the page ever obtained a token: `setTurnstileToken` had no
 * callers and there was no widget, so setting TURNSTILE_SECRET refused
 * every question. This runs the Ask panel as the page mounts it, with
 * the network and Cloudflare's script standing in, and follows a
 * question from the settings to the turn that carries a token.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

import { QueryView } from '../src/components/QueryView';
import type { DatasetClient } from '../src/d1/client';
import { DICTIONARIES } from '../src/i18n/strings';

const SITE_KEY = '0x4AAAAAAA-the-site-key';

/** A dataset with nothing in it, answering in the shapes the page expects. */
const ANSWERS: Record<string, unknown> = {
  countDependents: 0,
  versionSpread: { versions: [], constrained: 0, unversioned: 0 },
  dependencyTree: { root: 'mail', children: [] },
  edgeAmbiguity: null,
};

const dataset = (): DatasetClient =>
  new Proxy(
    {},
    {
      get(_target, key: string) {
        return () => Promise.resolve(key in ANSWERS ? ANSWERS[key] : []);
      },
    },
  ) as DatasetClient;

const json = (payload: unknown, status = 200) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });

beforeEach(() => {
  cleanup();
  document.head.innerHTML = '';
  Reflect.deleteProperty(window, 'turnstile');
});
// Unmounted after the test as well as before the next: the view's
// debounce timer otherwise fires after jsdom is torn down, and vitest
// exits 1 on "window is not defined" with every test passed.
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

it('shows the widget in the Ask panel and sends its token with the first turn', async () => {
  const posted: Array<Record<string, unknown>> = [];
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: string, init?: RequestInit) => {
      if ((init?.method ?? 'GET') === 'GET') return json({ turnstile: { siteKey: SITE_KEY } });
      posted.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
      return json({
        id: 'm1',
        stop_reason: 'end_turn',
        usage: { input_tokens: 1, output_tokens: 1 },
        content: [{ type: 'text', text: 'Seventeen.' }],
        session: 'session-1',
      });
    }),
  );
  const rendered: HTMLElement[] = [];
  const turnstile = {
    render: vi.fn((container: HTMLElement, params: { sitekey: string; callback(token: string): void }) => {
      rendered.push(container);
      expect(params.sitekey).toBe(SITE_KEY);
      setTimeout(() => params.callback('token-1'), 0);
      return 'widget-1';
    }),
    remove: vi.fn(),
  };

  render(
    <QueryView
      words={DICTIONARIES.en}
      locale="en"
      dataset={dataset()}
      languages={[]}
      route={{ view: 'query', package: 'mail' }}
      go={vi.fn()}
    />,
  );
  fireEvent.change(screen.getByLabelText('Question'), { target: { value: 'who declares mail?' } });
  fireEvent.click(screen.getByRole('button', { name: 'Ask' }));

  // Cloudflare's script, added only now that the Worker has asked for it.
  const script = await waitFor(() => {
    const found = document.querySelector<HTMLScriptElement>(
      'script[src^="https://challenges.cloudflare.com/turnstile/v0/api.js"]',
    );
    expect(found).not.toBeNull();
    return found!;
  });
  Object.assign(window, { turnstile });
  script.dispatchEvent(new Event('load'));

  await waitFor(() => expect(screen.getByText('Seventeen.')).toBeTruthy());
  expect(posted).toEqual([expect.objectContaining({ turnstileToken: 'token-1' })]);

  // Drawn inside the panel the question was asked in.
  const panel = screen.getByLabelText('Question').closest('section, .rail');
  expect(rendered).toHaveLength(1);
  expect(panel?.contains(rendered[0]!)).toBe(true);
});
