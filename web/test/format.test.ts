/**
 * A number, written the way the chosen language writes it (#43).
 *
 * `toLocaleString()` with no argument writes a number in the runtime's
 * locale: the browser's, not the one the reader picked on the page.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import { formatNumber } from '../src/i18n/format';
import { germanDefault } from './locales';

afterEach(() => vi.restoreAllMocks());

describe('formatNumber', () => {
  it('writes a number as Intl writes it in the language chosen', () => {
    for (const n of [0, 7, 1234, 24_339, 19_502_430]) {
      expect(formatNumber(n, 'en')).toBe(new Intl.NumberFormat('en').format(n));
      // The tag `document.documentElement.lang` carries, so the page and
      // its numbers name the same locale.
      expect(formatNumber(n, 'zh')).toBe(new Intl.NumberFormat('zh-CN').format(n));
    }
    expect(formatNumber(1234, 'en')).toBe('1,234');
  });

  it('never falls back on the browser’s own locale', () => {
    // A German browser writes 24.339 when asked for no locale in
    // particular; a reader who chose English reads 24,339.
    germanDefault();
    expect((24_339).toLocaleString()).toBe('24.339');
    expect(formatNumber(24_339, 'en')).toBe('24,339');
    expect(formatNumber(24_339, 'zh')).toBe(
      new Intl.NumberFormat('zh-CN').format(24_339),
    );
  });

  it('makes one formatter per language, however many numbers it writes', async () => {
    // A chart formats a number for every mark, and building an
    // `Intl.NumberFormat` is the expensive part.
    vi.resetModules();
    const made = vi.spyOn(Intl, 'NumberFormat');
    const fresh = await import('../src/i18n/format');
    for (let n = 0; n < 50; n += 1) {
      fresh.formatNumber(n, 'en');
      fresh.formatNumber(n, 'zh');
    }
    expect(made).toHaveBeenCalledTimes(2);
  });
});
