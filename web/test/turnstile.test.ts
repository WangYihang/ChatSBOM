/**
 * The Turnstile widget, as the page shows it (#32).
 *
 * There was none: `Agent.setTurnstileToken` had no callers, and a
 * deployment that set TURNSTILE_SECRET refused every question. The
 * script is Cloudflare's and runs only in a browser, so these stand in
 * for it with the part of its API the page uses, and check what the
 * page does with that: when it loads the script, where the widget goes,
 * and what comes back.
 */
// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

const SCRIPT = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';

/** What the Worker says a question must pass (#115). */
const CHALLENGE = { siteKey: 'the-site-key', action: 'ask' };

type Params = Record<string, unknown> & {
  sitekey: string;
  callback(token: string): void;
  'error-callback'(code: string): void;
  'timeout-callback'(): void;
};

/**
 * `window.turnstile`, rendering a widget that settles as `outcome` says
 * — later, as a challenge does — and recording what it was asked.
 */
function fakeTurnstile(outcome: (params: Params, count: number) => void) {
  const rendered: Array<{ container: HTMLElement; params: Params }> = [];
  const removed: string[] = [];
  const api = {
    render: vi.fn((container: HTMLElement, params: Params) => {
      rendered.push({ container, params });
      const count = rendered.length;
      setTimeout(() => outcome(params, count), 0);
      return `widget-${count}`;
    }),
    remove: vi.fn((id: string) => void removed.push(id)),
  };
  return { api, rendered, removed };
}

/** The script tags the page has added for Turnstile. */
const scripts = () =>
  Array.from(document.querySelectorAll('script')).filter((script) =>
    script.src.startsWith('https://challenges.cloudflare.com/'),
  );

/** What the browser does once the script has run. */
function scriptLoads(api: unknown) {
  Object.assign(window, { turnstile: api });
  scripts().at(-1)!.dispatchEvent(new Event('load'));
}

async function solver() {
  const { turnstileSolver } = await import('../src/ask/turnstile');
  return turnstileSolver(() => document.getElementById('host'), () => 'en');
}

beforeEach(() => {
  // The script is loaded once per page, so each test starts a new page.
  vi.resetModules();
  document.head.innerHTML = '';
  document.body.innerHTML = '<div id="host"></div>';
  Reflect.deleteProperty(window, 'turnstile');
});

describe('the widget', () => {
  it('loads nothing from Cloudflare until a token is asked for', async () => {
    await solver();
    expect(scripts()).toHaveLength(0);
  });

  it('loads Cloudflare’s script once, for explicit rendering', async () => {
    const { api } = fakeTurnstile((params, count) => params.callback(`token-${count}`));
    const solve = await solver();

    const first = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    expect(scripts()[0]!.src).toBe(SCRIPT);
    scriptLoads(api);

    await expect(first).resolves.toBe('token-1');
    await expect(solve(CHALLENGE)).resolves.toBe('token-2');
    expect(scripts()).toHaveLength(1);
  });

  it('renders into the page’s host with the site key and the action, and takes it down once solved', async () => {
    const { api, rendered, removed } = fakeTurnstile((params) => params.callback('token-1'));
    const solve = await solver();

    const token = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    scriptLoads(api);
    await expect(token).resolves.toBe('token-1');

    expect(rendered).toHaveLength(1);
    expect(rendered[0]!.container).toBe(document.getElementById('host'));
    expect(rendered[0]!.params).toMatchObject({
      sitekey: 'the-site-key',
      // What siteverify names back, and the Worker checks (#115).
      action: 'ask',
      // Seen only when Cloudflare wants a person to click.
      appearance: 'interaction-only',
      // No form here to put a hidden input in.
      'response-field': false,
    });
    expect(removed).toEqual(['widget-1']);
  });

  it('solves a fresh challenge for every token', async () => {
    const { api, rendered } = fakeTurnstile((params, count) => params.callback(`token-${count}`));
    const solve = await solver();

    const first = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    scriptLoads(api);

    expect([await first, await solve(CHALLENGE)]).toEqual(['token-1', 'token-2']);
    expect(rendered).toHaveLength(2);
  });

  it.each([
    ['fails', (params: Params) => params['error-callback']('300030'), /verification failed/i],
    ['is left unsolved', (params: Params) => params['timeout-callback'](), /timed out/i],
  ])('rejects when the challenge %s, and takes the widget down', async (_, outcome, reason) => {
    const { api, removed } = fakeTurnstile(outcome);
    const solve = await solver();

    const token = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    scriptLoads(api);

    await expect(token).rejects.toThrow(reason);
    expect(removed).toEqual(['widget-1']);
  });

  it('rejects when the script cannot be loaded, and tries again next time', async () => {
    const solve = await solver();

    const token = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    const failed = scripts()[0]!;
    failed.dispatchEvent(new Event('error'));
    await expect(token).rejects.toThrow(/could not be loaded/i);
    expect(scripts()).toHaveLength(0);

    const { api } = fakeTurnstile((params) => params.callback('token-1'));
    const again = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    expect(scripts()[0]).not.toBe(failed);
    scriptLoads(api);
    await expect(again).resolves.toBe('token-1');
  });

  it('draws the widget in the language the page speaks, not the browser’s (#43)', async () => {
    // Left to itself the widget follows the browser. The language is
    // asked for each challenge, so a switch between two questions
    // reaches the next one.
    const { api, rendered } = fakeTurnstile((params, count) => params.callback(`token-${count}`));
    const { turnstileSolver } = await import('../src/ask/turnstile');
    let locale: 'en' | 'zh' = 'zh';
    const solve = turnstileSolver(() => document.getElementById('host'), () => locale);

    const first = solve(CHALLENGE);
    await vi.waitFor(() => expect(scripts()).toHaveLength(1));
    scriptLoads(api);
    await first;
    locale = 'en';
    await solve(CHALLENGE);

    // Cloudflare's names for them.
    expect(rendered.map(({ params }) => params['language'])).toEqual(['zh-cn', 'en']);
  });

  it('rejects when the page has nowhere to show it', async () => {
    const { api } = fakeTurnstile((params) => params.callback('token-1'));
    Object.assign(window, { turnstile: api });
    const { turnstileSolver } = await import('../src/ask/turnstile');

    await expect(turnstileSolver(() => null, () => 'en')(CHALLENGE)).rejects.toThrow();
    expect(api.render).not.toHaveBeenCalled();
  });
});
