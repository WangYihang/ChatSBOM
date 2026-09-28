/**
 * Cloudflare Turnstile, for a deployment that requires it (#32).
 *
 * Loaded only then. A deployment without TURNSTILE_SECRET never loads
 * anything from Cloudflare, and one with it loads the script when the
 * first question is asked rather than with the page. The widget is
 * drawn in a host the page provides, in the Ask panel, one challenge
 * per token: a token is good for one turn, and the next question draws
 * a fresh widget. `interaction-only` keeps it out of sight unless
 * Cloudflare wants a person to click.
 *
 * The page's policy allows exactly this: the script from
 * challenges.cloudflare.com, and the frame it draws (`public/_headers`).
 */

/** Explicit rendering: the page decides when, and where. */
const SCRIPT = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';

/** The part of Turnstile's API the page uses. */
interface Turnstile {
  render(container: HTMLElement, params: WidgetParams): string | null | undefined;
  remove(widgetId: string): void;
}

interface WidgetParams {
  sitekey: string;
  appearance: 'interaction-only';
  /** No form here to put a hidden input in. */
  'response-field': false;
  /** A failure ends the attempt; asking again draws a new widget. */
  retry: 'never';
  /** Nothing waits on a token once it is taken, so none is refreshed. */
  'refresh-expired': 'never';
  callback(token: string): void;
  'error-callback'(code: string): boolean;
  'timeout-callback'(): void;
}

declare global {
  interface Window {
    turnstile?: Turnstile;
  }
}

let loading: Promise<Turnstile> | undefined;

/** Cloudflare's script, added once, and again only after a failed load. */
function load(): Promise<Turnstile> {
  loading ??= new Promise<Turnstile>((resolve, reject) => {
    const script = document.createElement('script');
    const fail = () => {
      loading = undefined;
      script.remove();
      reject(new Error('The human verification check could not be loaded. Reload and retry.'));
    };
    script.src = SCRIPT;
    script.async = true;
    script.addEventListener('load', () =>
      window.turnstile ? resolve(window.turnstile) : fail(),
    );
    script.addEventListener('error', fail);
    document.head.appendChild(script);
  });
  return loading;
}

/**
 * Solves a challenge each time it is called, in the element `host`
 * returns, and resolves with the token.
 */
export function turnstileSolver(
  host: () => HTMLElement | null,
): (siteKey: string) => Promise<string> {
  return async (siteKey) => {
    const container = host();
    if (!container) {
      throw new Error('The human verification check has nowhere to show. Reload and retry.');
    }
    const turnstile = window.turnstile ?? (await load());

    return new Promise<string>((resolve, reject) => {
      let settled = false;
      // Taken down once it has answered, either way. Queued, so that it
      // happens after `render` has returned the id to take down.
      const settle = (finish: () => void) => {
        if (settled) return;
        settled = true;
        queueMicrotask(() => {
          if (widget) turnstile.remove(widget);
          finish();
        });
      };

      const widget = turnstile.render(container, {
        sitekey: siteKey,
        appearance: 'interaction-only',
        'response-field': false,
        retry: 'never',
        'refresh-expired': 'never',
        callback: (token) => settle(() => resolve(token)),
        'error-callback': (code) => {
          settle(() =>
            reject(new Error(`Human verification failed (${code}). Reload and retry.`)),
          );
          // Handled: Turnstile need not report it again.
          return true;
        },
        'timeout-callback': () =>
          settle(() => reject(new Error('Human verification timed out. Ask again.'))),
      });
      if (!widget) {
        settle(() =>
          reject(new Error('The human verification check could not be shown. Reload and retry.')),
        );
      }
    });
  };
}
