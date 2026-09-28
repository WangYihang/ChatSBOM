/**
 * A browser whose own locale is German, for the tests of #43.
 *
 * Where the page writes a number without naming a locale, the runtime's
 * default decides, and a German default writes 24.339 where English
 * writes 24,339. The machine running these tests is English, like the
 * page's own default, so a call that forgot the locale looked right on
 * it. Under this it does not.
 *
 * Restored by `vi.restoreAllMocks()`.
 */
import { vi } from 'vitest';

export function germanDefault(): void {
  const toLocaleString = Number.prototype.toLocaleString;
  vi.spyOn(Number.prototype, 'toLocaleString').mockImplementation(function (
    this: number,
    locales?: Intl.LocalesArgument,
    options?: Intl.NumberFormatOptions,
  ) {
    return toLocaleString.call(this, locales ?? 'de-DE', options);
  });

  const NumberFormat = Intl.NumberFormat;
  vi.spyOn(Intl, 'NumberFormat').mockImplementation(function (
    locales?: Intl.LocalesArgument,
    options?: Intl.NumberFormatOptions,
  ) {
    return new NumberFormat(locales ?? 'de-DE', options);
  } as typeof Intl.NumberFormat);
}
