/**
 * What the page is served with (#31).
 *
 * Not the Worker's to set: `run_worker_first` names /api/* alone, so the
 * page and its assets are answered by Cloudflare's asset server, which
 * reads `_headers` from the root of the assets — `public/` here, copied
 * there by the build — under `wrangler dev` as in a deployment. There
 * was no such file. The page went out with no policy, content-hashed
 * files with `max-age=0, must-revalidate`, a stylesheet from Google
 * Fonts, and 1.5 MB of source maps beside the bundle.
 */
import { describe, expect, it } from 'vitest';

import html from '../index.html?raw';
import assetsIgnore from '../public/.assetsignore?raw';
import headersFile from '../public/_headers?raw';
import viteConfig from '../vite.config';

/**
 * `_headers` as the asset server reads it: a line starting with `/` opens
 * a rule, the `Name: value` lines under it are its headers, and `#`
 * starts a comment. Names are case-insensitive, so they are lowercased.
 */
function rules(text: string): Map<string, Map<string, string>> {
  const byPath = new Map<string, Map<string, string>>();
  let current: Map<string, string> | undefined;
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (line === '' || line.startsWith('#')) continue;
    if (line.startsWith('/')) {
      current = new Map();
      byPath.set(line, current);
      continue;
    }
    const colon = line.indexOf(':');
    if (!current || colon < 1) throw new Error(`not a header line: ${line}`);
    current.set(line.slice(0, colon).trim().toLowerCase(), line.slice(colon + 1).trim());
  }
  return byPath;
}

/** A policy's directives and their sources. */
function directives(policy: string): Map<string, string[]> {
  return new Map(
    policy
      .split(';')
      .map((part) => part.trim().split(/\s+/))
      .filter((words) => words[0])
      .map(([name, ...sources]) => [name!, sources]),
  );
}

const byPath = rules(headersFile);
const everything = byPath.get('/*') ?? new Map<string, string>();
const policy = everything.get('content-security-policy') ?? '';
const csp = directives(policy);

/** Where Turnstile's script and its frame come from (#32). */
const TURNSTILE = 'https://challenges.cloudflare.com';

describe('the policy', () => {
  it('lets the page load from its own origin alone, but for Turnstile', () => {
    expect(csp.get('default-src')).toEqual(["'self'"]);
    for (const directive of ['style-src', 'font-src', 'img-src', 'connect-src']) {
      expect([directive, csp.get(directive)]).toEqual([directive, ["'self'"]]);
    }
  });

  it('lets Turnstile run its script and show its frame, and no more (#32)', () => {
    // What Cloudflare documents the widget as needing. Its token comes
    // back through a message from the frame, not a request of the
    // page's, so connect-src stays this origin's alone.
    expect(csp.get('script-src')).toEqual(["'self'", TURNSTILE]);
    expect(csp.get('frame-src')).toEqual([TURNSTILE]);
  });

  it('names no origin but Turnstile’s', () => {
    const origins = policy.match(/\b[a-z][a-z0-9+.-]*:\/\/[^\s;]*/gi) ?? [];
    expect(new Set(origins)).toEqual(new Set([TURNSTILE]));
  });

  it('allows nothing inline, nothing evaluated, no plugin and no <base>', () => {
    expect(policy).not.toMatch(/'unsafe-|'strict-dynamic'|\bdata:|\bblob:|\*/);
    expect(csp.get('object-src')).toEqual(["'none'"]);
    expect(csp.get('base-uri')).toEqual(["'none'"]);
    // The one form, the chat's, submits in script; a native submit
    // would only reload the page.
    expect(csp.get('form-action')).toEqual(["'none'"]);
  });

  it('may not be framed by another page', () => {
    expect(csp.get('frame-ancestors')).toEqual(["'none'"]);
  });
});

describe('the other headers', () => {
  it('forbids sniffing a type the response did not declare', () => {
    expect(everything.get('x-content-type-options')).toBe('nosniff');
  });

  it('tells a site it links to where the reader came from, and no more', () => {
    expect(everything.get('referrer-policy')).toBe('strict-origin-when-cross-origin');
  });

  it('caches the content-hashed assets for good', () => {
    expect(byPath.get('/assets/*')?.get('cache-control')).toBe(
      'public, max-age=31536000, immutable',
    );
  });

  it('sets no cache policy for everything, which /assets/* would inherit joined to its own', () => {
    // Two rules naming one header are joined with a comma, and
    // /assets/x matches both: `no-cache, public, max-age=...` is
    // whatever a cache makes of it.
    expect(everything.has('cache-control')).toBe(false);
  });
});

describe('the page itself', () => {
  it('loads nothing from another origin', () => {
    // The fonts were Google's; they are part of the build now.
    expect(html).not.toMatch(/https?:\/\//);
  });

  it('has nothing inline for the policy to refuse', () => {
    const scripts = html.match(/<script\b[^>]*>/g) ?? [];
    expect(scripts.length).toBeGreaterThan(0);
    for (const script of scripts) expect(script).toMatch(/\ssrc=/);
    expect(html).not.toMatch(/<style\b/i);
    expect(html).not.toMatch(/\sstyle=/i);
    expect(html).not.toMatch(/\son[a-z]+=/i);
  });
});

describe('source maps', () => {
  it('are written for debugging, but the bundle does not point at them', () => {
    expect(viteConfig.build?.sourcemap).toBe('hidden');
  });

  it('are not uploaded with the assets, so they are not served', () => {
    expect(assetsIgnore.split('\n').map((line) => line.trim())).toContain('*.map');
  });
});
