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
 *
 * The widget is rendered with the site key and the action the Worker
 * names, and siteverify tells the Worker both back: a token solved for
 * another action, or on another site's page, is refused (#115).
 */
import type { Challenge } from '../agent';
import type { Locale } from '../i18n/locale';

/** Explicit rendering: the page decides when, and where. */
const SCRIPT = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';

/**
 * Cloudflare's name for each language the page speaks. Left to itself
 * the widget speaks the browser's, whatever the page was switched to
 * (#43).
 */
const LANGUAGES: Readonly<Record<Locale, string>> = { en: 'en', zh: 'zh-cn' };

/**
 * A challenge that could not be passed, and at which step.
 *
 * The message is English, for the log and the English page; the step is
 * what a page in another language says it by (#43).
 */
export class VerificationError extends Error {
  constructor(
    message: string,
    readonly step: 'load' | 'show' | 'failed' | 'timeout',
    /** Cloudflare's code, for a challenge that failed. */
    readonly code: string | null = null,
  ) {
    super(message);
  }
}

/** The part of Turnstile's API the page uses. */
interface Turnstile {
  render(container: HTMLElement, params: WidgetParams): string | null | undefined;
  remove(widgetId: string): void;
}

interface WidgetParams {
  sitekey: string;
  /** What siteverify names back, for the Worker to check. */
  action: string;
  appearance: 'interaction-only';
  /** The page's language, not the browser's. */
  language: string;
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
      reject(
        new VerificationError(
          'The human verification check could not be loaded. Reload and retry.',
          'load',
        ),
      );
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
 * returns and the language `locale` returns, and resolves with the
 * token. Both are asked each time: the host can be redrawn, and the
 * reader can switch language between two questions.
 */
export function turnstileSolver(
  host: () => HTMLElement | null,
  locale: () => Locale,
): (challenge: Challenge) => Promise<string> {
  return async ({ siteKey, action }) => {
    const container = host();
    if (!container) {
      throw new VerificationError(
        'The human verification check has nowhere to show. Reload and retry.',
        'show',
      );
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
        action,
        appearance: 'interaction-only',
        language: LANGUAGES[locale()],
        'response-field': false,
        retry: 'never',
        'refresh-expired': 'never',
        callback: (token) => settle(() => resolve(token)),
        'error-callback': (code) => {
          settle(() =>
            reject(
              new VerificationError(
                `Human verification failed (${code}). Reload and retry.`,
                'failed',
                code,
              ),
            ),
          );
          // Handled: Turnstile need not report it again.
          return true;
        },
        'timeout-callback': () =>
          settle(() =>
            reject(
              new VerificationError('Human verification timed out. Ask again.', 'timeout'),
            ),
          ),
      });
      if (!widget) {
        settle(() =>
          reject(
            new VerificationError(
              'The human verification check could not be shown. Reload and retry.',
              'show',
            ),
          ),
        );
      }
    });
  };
}
