/**
 * A question, answered as it streams (#144).
 *
 * The page posts the question to `/api/ask`, where the service runs the
 * model's loop and its tools (#140), and reads the answer as
 * server-sent events: `tool`, `text`, `done` and `error`. An
 * EventSource cannot post, so the page reads the body itself, and
 * these hold its reading to the standard's: lines ended by LF, CRLF or
 * CR, comments skipped, an event dispatched at a blank line, and one
 * the stream cut off before its blank line never dispatched.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { AskProgress } from '../src/ask/contract';
import {
  ask,
  AskError,
  earlier,
  events,
  MAX_PRIOR_CHARS,
  type Exchange,
} from '../src/ask/stream';

afterEach(() => vi.unstubAllGlobals());

/** A body that arrives in `chunks`, each as it is written. */
function body(...chunks: (string | Uint8Array)[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(typeof chunk === 'string' ? encoder.encode(chunk) : chunk);
      }
      controller.close();
    },
  });
}

async function read(stream: ReadableStream<Uint8Array>) {
  const found: { event: string; data: string }[] = [];
  for await (const event of events(stream)) found.push(event);
  return found;
}

/** One event as the service writes it (`chatsbom/server/ask.py`, `sse`). */
const sse = (event: string, data: unknown) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

describe('the events of a stream', () => {
  it('are read at each blank line, a name and its data', async () => {
    expect(await read(body(sse('text', { delta: 'Four' }), sse('done', { turns: 1 })))).toEqual([
      { event: 'text', data: '{"delta":"Four"}' },
      { event: 'done', data: '{"turns":1}' },
    ]);
  });

  it('are read whatever the chunks the body arrives in', async () => {
    const whole = sse('text', { delta: 'déjà vu 🚀' }) + sse('done', { turns: 2 });
    const bytes = new TextEncoder().encode(whole);
    // A byte at a time: lines, and characters, split across chunks.
    const pieces = [...bytes].map((byte) => Uint8Array.of(byte));
    expect(await read(body(...pieces))).toEqual([
      { event: 'text', data: '{"delta":"déjà vu 🚀"}' },
      { event: 'done', data: '{"turns":2}' },
    ]);
  });

  it('skip comments, the keep-alive among them', async () => {
    expect(
      await read(body(': keep-alive\n\n', ':\n', sse('done', { turns: 1 }))),
    ).toEqual([{ event: 'done', data: '{"turns":1}' }]);
  });

  it('take lines ended by CRLF or CR as by LF', async () => {
    expect(
      await read(body('event: text\r\ndata: {"delta":"a"}\r\n\r\n', 'event: done\rdata: {}\r\r')),
    ).toEqual([
      { event: 'text', data: '{"delta":"a"}' },
      { event: 'done', data: '{}' },
    ]);
  });

  it('keep a CR last in a chunk until the next says whether it was a CRLF', async () => {
    expect(await read(body('event: done\r', '\ndata: {}\r', '\n\r', '\n'))).toEqual([
      { event: 'done', data: '{}' },
    ]);
  });

  it('join data lines, and take a field with no space after its colon', async () => {
    expect(await read(body('event:text\ndata:one\ndata: two\n\n'))).toEqual([
      { event: 'text', data: 'one\ntwo' },
    ]);
  });

  it('name an event that names none a message, and drop one with no data', async () => {
    expect(await read(body('data: plain\n\n', 'event: empty\n\n'))).toEqual([
      { event: 'message', data: 'plain' },
    ]);
  });

  it('never dispatch an event the stream ended before its blank line', async () => {
    expect(await read(body(sse('text', { delta: 'a' }), 'event: done\ndata: {"turns":1}\n'))).toEqual([
      { event: 'text', data: '{"delta":"a"}' },
    ]);
  });
});

/** The service at `/api/ask`, answering with `reply`; what it was sent, kept. */
function stubAsk(reply: () => Response) {
  const sent: { url: string; init: RequestInit }[] = [];
  vi.stubGlobal('fetch', (url: string, init: RequestInit) => {
    sent.push({ url, init });
    return Promise.resolve(reply());
  });
  return sent;
}

const stream = (...chunks: string[]) =>
  new Response(body(...chunks), {
    headers: { 'content-type': 'text/event-stream; charset=utf-8' },
  });

const refusal = (status: number, payload: unknown) =>
  new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });

const ASKED = { question: 'Who declares mail?', prior: [], altcha: 'c29sdmVk' };

/** A question's failure, as the page is told it. */
async function failure(asking: Promise<unknown>): Promise<AskError> {
  const error: unknown = await asking.then(
    () => expect.fail('answered'),
    (thrown: unknown) => thrown,
  );
  expect(error).toBeInstanceOf(AskError);
  return error as AskError;
}

describe('a question', () => {
  it('is posted as JSON, with its earlier exchanges and its solved challenge', async () => {
    const sent = stubAsk(() => stream(sse('done', { turns: 1 })));
    const prior = [{ q: 'Who declares mail?', a: 'Four projects.' }];
    await ask({ ...ASKED, question: 'And rails?', prior });
    expect(sent).toHaveLength(1);
    expect(sent[0]!.url).toBe('/api/ask');
    expect(sent[0]!.init.method).toBe('POST');
    expect(new Headers(sent[0]!.init.headers).get('content-type')).toBe('application/json');
    expect(JSON.parse(String(sent[0]!.init.body))).toEqual({
      question: 'And rails?',
      prior,
      altcha: 'c29sdmVk',
    });
  });

  it('is answered with the text its events wrote, once it is done', async () => {
    stubAsk(() =>
      stream(
        ': keep-alive\n\n',
        sse('text', { delta: 'Four projects ' }),
        sse('text', { delta: 'declare mail.' }),
        sse('done', { turns: 1, usage: {}, cost_usd: 0.0001 }),
      ),
    );
    expect(await ask(ASKED)).toBe('Four projects declare mail.');
  });

  it('says each tool as it is run, and the answer as it is written', async () => {
    stubAsk(() =>
      stream(
        sse('text', { delta: 'Let me look.' }),
        sse('tool', { name: 'ecosystems_for', arguments: { name: 'mail' } }),
        sse('tool', { name: 'dependents_of', arguments: { name: 'mail', type: 'gem' } }),
        sse('text', { delta: 'Four ' }),
        sse('text', { delta: 'gems.' }),
        sse('done', { turns: 2 }),
      ),
    );
    const heard: [string, unknown][] = [];
    const progress: AskProgress = {
      onThinking: (text) => heard.push(['thinking', text]),
      onToolCall: (name, input) => heard.push(['tool', [name, input]]),
      onText: (text) => heard.push(['text', text]),
    };
    // What a turn wrote before it called tools is not the answer: it is
    // said as the model's thinking aloud, as the Worker's turns were.
    expect(await ask(ASKED, progress)).toBe('Four gems.');
    expect(heard).toEqual([
      ['text', 'Let me look.'],
      ['thinking', 'Let me look.'],
      ['text', ''],
      ['tool', ['ecosystems_for', { name: 'mail' }]],
      ['tool', ['dependents_of', { name: 'mail', type: 'gem' }]],
      ['text', 'Four '],
      ['text', 'Four gems.'],
    ]);
  });

  it('passes over an event it does not know', async () => {
    stubAsk(() =>
      stream(sse('thinking', { text: 'Hmm.' }), sse('text', { delta: 'Yes.' }), sse('done', {})),
    );
    expect(await ask(ASKED)).toBe('Yes.');
  });

  it.each([
    ['cut-off', 'The answer was cut off at its length limit before it finished. Try a narrower question.', {}],
    ['model', 'The model could not be reached. Try again shortly.', {}],
    ['stopped', 'The model stopped without an answer (insufficient_system_resource).', { reason: 'insufficient_system_resource' }],
    ['turns', 'Gave up after 8 turns without a final answer.', {}],
  ])('fails with the code of an error event: %s', async (code, message, more) => {
    stubAsk(() =>
      stream(
        sse('text', { delta: 'Partly' }),
        sse('error', { code, message, turns: 8, ...more }),
      ),
    );
    const error = await failure(ask(ASKED));
    expect(error.failure).toEqual({ code, said: message, status: null, turns: 8, ...more });
    expect(error.message).toBe(message);
  });

  it.each([
    [429, { error: 'Too many questions. Wait a moment.', code: 'rate' }],
    [503, { error: 'AI answers are busy. Try again in a moment.', code: 'busy' }],
    [400, { error: 'Question too long: 4001 characters, limit 4000.', code: 'invalid' }],
    [403, { error: 'Human verification failed. Reload and retry.', code: 'verification-failed', verdict: 'expired' }],
  ])('fails with the code of a refusal: %i', async (status, payload) => {
    stubAsk(() => refusal(status, payload));
    const error = await failure(ask(ASKED));
    expect(error.failure).toEqual({
      code: payload.code,
      said: payload.error,
      status,
      ...('verdict' in payload ? { verdict: payload.verdict } : {}),
    });
  });

  it('fails with the status of a refusal that says no code', async () => {
    stubAsk(() => new Response('<html>Bad gateway</html>', { status: 502 }));
    const error = await failure(ask(ASKED));
    expect(error.failure).toEqual({
      code: 'refused',
      said: 'The question was refused (502).',
      status: 502,
    });
  });

  it('fails as not understood when the answer is not a stream of events', async () => {
    stubAsk(() => refusal(200, { answer: 'Four.' }));
    expect((await failure(ask(ASKED))).failure.code).toBe('garbled');
  });

  it('fails as not understood when an event is not JSON', async () => {
    stubAsk(() => stream('event: text\ndata: Four\n\n'));
    expect((await failure(ask(ASKED))).failure.code).toBe('garbled');
  });

  it('fails as cut off when the stream ends before it is done', async () => {
    stubAsk(() => stream(sse('text', { delta: 'Four proj' })));
    const error = await failure(ask(ASKED));
    expect(error.failure.code).toBe('interrupted');
    expect(error.failure.status).toBeNull();
  });

  it('fails as it failed when the service could not be reached', async () => {
    vi.stubGlobal('fetch', () => Promise.reject(new TypeError('Failed to fetch')));
    await expect(ask(ASKED)).rejects.toThrow(TypeError);
  });
});

/**
 * What a question brings of the conversation before it: the service
 * takes at most three earlier exchanges, 12,000 characters in all, as
 * JavaScript counts them, and refuses a question with more (#140).
 */
describe('the earlier exchanges a question brings', () => {
  const exchange = (n: number, size = 10): Exchange => ({
    q: `q${n}`.padEnd(size, '?'),
    a: `a${n}`.padEnd(size, '.'),
  });

  it('are none for the first question', () => {
    expect(earlier([])).toEqual([]);
  });

  it('are the last three, in the order they were asked', () => {
    const conversation = [1, 2, 3, 4, 5].map((n) => exchange(n));
    expect(earlier(conversation)).toEqual([exchange(3), exchange(4), exchange(5)]);
  });

  it('are as many of the last as fit, the ones before a long one left out', () => {
    const long = { q: 'q', a: 'x'.repeat(MAX_PRIOR_CHARS - 100) };
    expect(earlier([exchange(1), long, exchange(3, 40)])).toEqual([long, exchange(3, 40)]);
    expect(earlier([long, exchange(2, 60)])).toEqual([exchange(2, 60)]);
  });

  it('keep the newest cut to fit, rather than leave out what a follow-up is about', () => {
    const newest = { q: 'Who declares mail?', a: 'x'.repeat(20_000) };
    const [kept, ...rest] = earlier([exchange(1), newest]);
    expect(rest).toEqual([]);
    expect(kept!.q).toBe(newest.q);
    expect(kept!.a.endsWith('…')).toBe(true);
    expect(kept!.q.length + kept!.a.length).toBe(MAX_PRIOR_CHARS);
  });

  it('never cut a character in half', () => {
    // A rocket is two units, and half of one is not text to the
    // service; the room left for the answer ends inside one here.
    const newest = { q: 'qq', a: '🚀'.repeat(10_000) };
    const [kept] = earlier([newest]);
    expect(kept!.q.length + kept!.a.length).toBeLessThanOrEqual(MAX_PRIOR_CHARS);
    // Which throws on half a pair.
    expect(() => encodeURIComponent(kept!.a)).not.toThrow();
  });

  it('never bring more than the service takes', () => {
    const conversation = Array.from({ length: 6 }, (_, n) => exchange(n, 3_000));
    const brought = earlier(conversation);
    expect(brought.length).toBeLessThanOrEqual(3);
    expect(brought.reduce((sum, { q, a }) => sum + q.length + a.length, 0)).toBeLessThanOrEqual(
      MAX_PRIOR_CHARS,
    );
    expect(brought).toEqual(conversation.slice(-2));
  });
});
