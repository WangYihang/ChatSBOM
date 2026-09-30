/**
 * Every way a question can fail, in the reader's language (#144).
 *
 * The service says why a question got no answer by a code, in a refusal
 * or an `error` event (`chatsbom/server/ask.py`, and `app.py` for the
 * chat that is off), with an English sentence for the log beside it.
 * The page says each code in its own words, in both languages (#43), as
 * it says its queries' failures by their status. So the codes are read
 * here from the service's own source, and each has to have words of
 * its own in both dictionaries: a code the service learns and the page
 * does not fails here, rather than reaching a Chinese reader as English.
 */
import { readFileSync } from 'node:fs';

import { describe, expect, it } from 'vitest';

import type { AskFailure } from '../src/ask/stream';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

const source = (path: string) =>
  readFileSync(decodeURIComponent(new URL(`../../${path}`, import.meta.url).pathname), 'utf8');

const ASK = source('chatsbom/server/ask.py');
const APP = source('chatsbom/server/app.py');
const CHALLENGE = source('chatsbom/server/challenge.py');

const found = (text: string, pattern: RegExp) =>
  [...text.matchAll(pattern)].map((match) => match[1]!);

/** Every code the service says a question failed with. */
const CODES = [
  ...new Set([
    // A refusal: `refused(403, CROSS_ORIGIN, 'origin')`.
    ...found(ASK, /refused\(\s*\d+,\s*\w+,\s*'([a-z-]+)'/g),
    // A body that is not a question: `Invalid(..., code='size')`, and
    // its default.
    ...found(ASK, /code(?::\s*str)?\s*=\s*'([a-z-]+)'/g),
    // An error event: `self._error('cut-off', CUT_OFF)`.
    ...found(ASK, /_error\(\s*'([a-z-]+)'/g),
    // The chat that is off: `{'error': OFF, 'code': 'off'}`.
    ...found(APP, /'code':\s*'([a-z-]+)'/g),
  ]),
].sort();

/** Why a challenge failed: `Verdict`'s values, less the one that passed. */
const VERDICTS = found(
  CHALLENGE.slice(CHALLENGE.indexOf('class Verdict'), CHALLENGE.indexOf('def _subkey')),
  /^\s+[A-Z_]+ = '([a-z ]+)'$/gm,
).filter((verdict) => verdict !== 'verified');

/** The page's own: a check that could not be done, an answer cut short or not understood. */
const PAGES = ['unverified', 'interrupted', 'garbled', 'refused'];

/** What the service said, which a code's own words need not repeat. */
const SAID = 'The sentence the service said.';

/** A failure with every detail a code may carry. */
const failure = (code: string): AskFailure => ({
  code,
  said: SAID,
  status: code === 'refused' ? 502 : 400,
  verdict: 'expired',
  reason: 'insufficient_system_resource',
  turns: 8,
});

/** What neither dictionary knows: said as the service said it, around it. */
const unknown = (words: typeof EN) => words.askUnanswered({ ...failure('no-such-code'), status: 418 });

/** Codes whose words keep the service's sentence, since only it says what was wrong. */
const KEEPS_SAID = new Set(['invalid']);

const LATIN = /[A-Za-z]/;
const CHINESE = /[一-鿿]/;

describe('the codes a question fails with', () => {
  it('are read from the service, all of them', () => {
    // If this breaks, the checks below are vacuous rather than failing.
    expect(CODES).toEqual([
      'budget', 'busy', 'cut-off', 'declined', 'failed', 'garbled', 'invalid', 'json',
      'model', 'off', 'origin', 'rate', 'size', 'stopped', 'timeout', 'turns', 'unavailable',
      'verification-failed', 'verification-required',
    ]);
    expect(VERDICTS).toEqual([
      'malformed', 'bad signature', 'wrong solution', 'expired', 'other client', 'replayed',
    ]);
  });

  it.each([...CODES, ...PAGES])('each have words of their own in English: %s', (code) => {
    const said = EN.askUnanswered(failure(code));
    expect(said).not.toBe('');
    if (KEEPS_SAID.has(code)) {
      expect(said).toContain(SAID);
    } else {
      expect(said).not.toBe(unknown(EN));
      expect(said).not.toContain(SAID);
    }
  });

  it.each([...CODES, ...PAGES])('and in Chinese: %s', (code) => {
    const said = ZH.askUnanswered(failure(code));
    expect(said).toMatch(CHINESE);
    expect(said).not.toBe(unknown(ZH));
    // English only where it is the detail the code carries: the
    // service's sentence of what was wrong, why the model stopped.
    const rest = said.replace(SAID, '').replace('insufficient_system_resource', '');
    expect(rest).not.toMatch(LATIN);
    if (!KEEPS_SAID.has(code)) expect(said).not.toContain(SAID);
  });

  it('say why the model stopped, and how many turns it had', () => {
    for (const words of [EN, ZH]) {
      expect(words.askUnanswered(failure('stopped'))).toContain('insufficient_system_resource');
      expect(words.askUnanswered(failure('turns'))).toContain('8');
    }
  });

  it.each(VERDICTS)('say a challenge that failed as %s in either language', (verdict) => {
    for (const words of [EN, ZH]) {
      const said = words.askUnanswered({ ...failure('verification-failed'), verdict });
      expect(said).not.toContain(SAID);
      expect(said).not.toBe(unknown(words));
    }
    expect(ZH.askUnanswered({ ...failure('verification-failed'), verdict })).not.toMatch(LATIN);
  });

  it('tell a challenge that ran out of time, or was used, from one that failed', () => {
    // Asking again is the remedy for the first two; the others take a
    // reload, and the reader is told which.
    const said = (verdict: string) =>
      EN.askUnanswered({ ...failure('verification-failed'), verdict });
    expect(new Set(['expired', 'replayed', 'other client', 'malformed'].map(said)).size).toBe(4);
  });

  it('say what they can of a code neither knows, in Chinese around it', () => {
    expect(unknown(EN)).toBe(SAID);
    expect(unknown(ZH)).toContain(SAID);
    expect(unknown(ZH)).toMatch(CHINESE);
  });
});
