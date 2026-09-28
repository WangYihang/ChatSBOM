/**
 * A number, written the way the page's language writes it.
 *
 * `toLocaleString()` with no argument writes a number in the runtime's
 * locale, which is the browser's and not the one the reader chose: about
 * fifteen call sites did, so a reader who picked English on a German
 * machine read 24.339 in a chart beside 24,339 in the panel over it
 * (#43). Every number the page shows is written here instead, for the
 * locale the reader chose.
 *
 * One formatter per locale, made the first time it is asked for: a
 * chart writes a number for every mark, and making an
 * `Intl.NumberFormat` is the costly part of writing one.
 */
import { type Locale, LOCALE_TAGS } from './locale';

const formats = new Map<Locale, Intl.NumberFormat>();

export function formatNumber(value: number, locale: Locale): string {
  let format = formats.get(locale);
  if (!format) {
    format = new Intl.NumberFormat(LOCALE_TAGS[locale]);
    formats.set(locale, format);
  }
  return format.format(value);
}
