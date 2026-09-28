import { describe, expect, it, vi } from 'vitest';

import { Agent, AgentError } from '../src/agent';
import type { DatasetClient } from '../src/d1/client';

/** A DatasetClient stub that records which tools the agent actually ran. */
function fakeDataset() {
  const calls: string[] = [];
  const dataset = {
    dependentsOf: async () => {
      calls.push('dependentsOf');
      return [
        {
          owner: 'mastodon', repo: 'mastodon', stars: 1, version: '2.9.0',
          url: '', relationship: 'direct' as const,
        },
      ];
    },
    // Part of dependents_of rather than a tool of its own, so not
    // recorded as one.
    countDependents: async () => 17,
    searchPackages: async () => {
      calls.push('searchPackages');
      return [{ name: 'mail', repositoryCount: 118, directCount: 17 }];
    },
    topPackages: async () => {
      calls.push('topPackages');
      return [];
    },
    versionSpread: async () => {
      calls.push('versionSpread');
      return [];
    },
    languageCoverage: async () => {
      calls.push('languageCoverage');
      return [];
    },
  } as unknown as DatasetClient;
  return { dataset, calls };
}

const USAGE = { input_tokens: 10, output_tokens: 5 };

function turn(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status });
}

function stubTurns(...responses: Response[]) {
  const fetchMock = vi.fn();
  responses.forEach((r) => fetchMock.mockResolvedValueOnce(r));
  vi.stubGlobal('fetch', fetchMock);
  return fetchMock;
}

describe('Agent', () => {
  it('returns the model text when no tools are called', async () => {
    stubTurns(
      turn({
        id: 'm1', stop_reason: 'end_turn', usage: USAGE,
        content: [{ type: 'text', text: '17 projects declare it.' }],
      }),
    );
    const { dataset, calls } = fakeDataset();
    const answer = await new Agent(dataset).ask('who declares mail?');

    expect(answer).toBe('17 projects declare it.');
    expect(calls).toEqual([]);
  });

  it('executes a tool call and feeds the result back', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm1', stop_reason: 'tool_use', usage: USAGE,
        content: [{
          type: 'tool_use', id: 'tu_1',
          name: 'dependents_of', input: { name: 'mail', direct_only: true },
        }],
      }),
      turn({
        id: 'm2', stop_reason: 'end_turn', usage: USAGE,
        content: [{ type: 'text', text: 'mastodon/mastodon declares it.' }],
      }),
    );

    const { dataset, calls } = fakeDataset();
    const answer = await new Agent(dataset).ask('who declares mail?');

    expect(calls).toEqual(['dependentsOf']);
    expect(answer).toContain('mastodon');

    // The second request must carry the tool_result keyed to tu_1 —
    // the answer, not an error standing in for it.
    const second = JSON.parse(fetchMock.mock.calls[1]![1].body);
    const results = second.messages.at(-1).content;
    expect(results[0]).toMatchObject({
      type: 'tool_result', tool_use_id: 'tu_1',
    });
    expect(results[0].is_error).toBeUndefined();
    expect(JSON.parse(results[0].content)).toMatchObject({
      total: 17, rows_shown: 1,
    });
  });

  it('answers parallel tool calls in a single user message', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm1', stop_reason: 'tool_use', usage: USAGE,
        content: [
          { type: 'tool_use', id: 'a', name: 'search_packages', input: { fragment: 'mail' } },
          { type: 'tool_use', id: 'b', name: 'language_coverage', input: {} },
        ],
      }),
      turn({
        id: 'm2', stop_reason: 'end_turn', usage: USAGE,
        content: [{ type: 'text', text: 'done' }],
      }),
    );

    const { dataset, calls } = fakeDataset();
    await new Agent(dataset).ask('compare');

    expect(calls.sort()).toEqual(['languageCoverage', 'searchPackages']);
    const second = JSON.parse(fetchMock.mock.calls[1]![1].body);
    expect(second.messages.at(-1).content).toHaveLength(2);
  });

  it('reports a failing tool back instead of dropping it', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm1', stop_reason: 'tool_use', usage: USAGE,
        content: [{ type: 'tool_use', id: 'x', name: 'no_such_tool', input: {} }],
      }),
      turn({
        id: 'm2', stop_reason: 'end_turn', usage: USAGE,
        content: [{ type: 'text', text: 'recovered' }],
      }),
    );

    const { dataset } = fakeDataset();
    await new Agent(dataset).ask('break it');

    const second = JSON.parse(fetchMock.mock.calls[1]![1].body);
    expect(second.messages.at(-1).content[0]).toMatchObject({
      type: 'tool_result', tool_use_id: 'x', is_error: true,
    });
  });

  it('surfaces thinking, text and tool events', async () => {
    stubTurns(
      turn({
        id: 'm1', stop_reason: 'tool_use', usage: USAGE,
        content: [
          { type: 'thinking', thinking: 'need the direct count' },
          { type: 'tool_use', id: 'a', name: 'dependents_of', input: { name: 'mail' } },
        ],
      }),
      turn({
        id: 'm2', stop_reason: 'end_turn', usage: USAGE,
        content: [{ type: 'text', text: 'final' }],
      }),
    );

    const thinking: string[] = [];
    const text: string[] = [];
    const tools: string[] = [];
    const { dataset } = fakeDataset();

    await new Agent(dataset, {
      onThinking: (t) => thinking.push(t),
      onText: (t) => text.push(t),
      onToolCall: (n) => tools.push(n),
    }).ask('q');

    expect(thinking).toEqual(['need the direct count']);
    expect(text).toEqual(['final']);
    expect(tools).toEqual(['dependents_of']);
  });

  it('stops after a bounded number of turns', async () => {
    const looping = () =>
      turn({
        id: 'm', stop_reason: 'tool_use', usage: USAGE,
        content: [{ type: 'tool_use', id: 'a', name: 'language_coverage', input: {} }],
      });
    stubTurns(...Array.from({ length: 12 }, looping));

    const { dataset } = fakeDataset();
    await expect(new Agent(dataset).ask('loop')).rejects.toThrow(AgentError);
  });

  it('surfaces the Worker error message', async () => {
    stubTurns(turn({ error: 'The daily budget is used up.' }, 429));
    const { dataset } = fakeDataset();
    await expect(new Agent(dataset).ask('q')).rejects.toThrow(/daily budget/);
  });

  it('keeps conversation history across questions', async () => {
    const fetchMock = stubTurns(
      turn({ id: 'm1', stop_reason: 'end_turn', usage: USAGE, content: [{ type: 'text', text: 'a' }] }),
      turn({ id: 'm2', stop_reason: 'end_turn', usage: USAGE, content: [{ type: 'text', text: 'b' }] }),
    );
    const { dataset } = fakeDataset();
    const agent = new Agent(dataset);
    await agent.ask('first');
    await agent.ask('second');

    const second = JSON.parse(fetchMock.mock.calls[1]![1].body);
    expect(second.messages).toHaveLength(3);
    expect(second.messages[0]).toMatchObject({ role: 'user', content: 'first' });
  });
});

/** The conversation a POST carried. */
const posted = (fetchMock: ReturnType<typeof stubTurns>, call: number) =>
  (JSON.parse(fetchMock.mock.calls[call]![1].body) as { messages: unknown[] }).messages;

const answer = (text: string) =>
  turn({ id: 'm', stop_reason: 'end_turn', usage: USAGE, content: [{ type: 'text', text }] });

describe('Agent: a question that fails leaves no trace (#42)', () => {
  /**
   * The question joined the conversation before its first turn was
   * sent and stayed there whatever became of it. So a refused question
   * rode along with every later one — a question too long for the
   * Worker was refused again on every question after it, and the page
   * had to be reloaded to ask anything.
   */
  it('drops a question whose turn was refused', async () => {
    const fetchMock = stubTurns(
      answer('first answer'),
      turn({ error: 'Too many questions. Wait a moment.' }, 429),
      answer('third answer'),
    );
    const agent = new Agent(fakeDataset().dataset);
    await agent.ask('first');
    await expect(agent.ask('second')).rejects.toThrow(/Too many questions/);
    await agent.ask('third');

    expect(posted(fetchMock, 2)).toEqual([
      { role: 'user', content: 'first' },
      { role: 'assistant', content: [{ type: 'text', text: 'first answer' }] },
      { role: 'user', content: 'third' },
    ]);
  });

  it('drops the tool turns of a question that failed part-way', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm1', stop_reason: 'tool_use', usage: USAGE,
        content: [{ type: 'tool_use', id: 'tu_1', name: 'language_coverage', input: {} }],
      }),
      turn({ error: 'The model could not be reached. Try again shortly.' }, 502),
      answer('recovered'),
    );
    const agent = new Agent(fakeDataset().dataset);
    await expect(agent.ask('first')).rejects.toThrow(/could not be reached/);
    await expect(agent.ask('second')).resolves.toBe('recovered');

    expect(posted(fetchMock, 2)).toEqual([{ role: 'user', content: 'second' }]);
  });

  it('drops a question the loop gave up on', async () => {
    const looping = () =>
      turn({
        id: 'm', stop_reason: 'tool_use', usage: USAGE,
        content: [{ type: 'tool_use', id: 'a', name: 'language_coverage', input: {} }],
      });
    const fetchMock = stubTurns(...Array.from({ length: 8 }, looping), answer('fine'));
    const agent = new Agent(fakeDataset().dataset);
    await expect(agent.ask('loop')).rejects.toThrow(AgentError);
    await agent.ask('next');

    expect(posted(fetchMock, 8)).toEqual([{ role: 'user', content: 'next' }]);
  });
});

describe('Agent: a new conversation (#42)', () => {
  /**
   * Past 40 messages or the Worker's character bound, every turn is
   * refused with "Start a new one" — and nothing on the page could.
   */
  it('forgets every question asked before it', async () => {
    const fetchMock = stubTurns(answer('a'), answer('b'));
    const agent = new Agent(fakeDataset().dataset);
    await agent.ask('first');
    agent.reset();
    await agent.ask('second');

    expect(posted(fetchMock, 1)).toEqual([{ role: 'user', content: 'second' }]);
  });
});

describe('Agent: why the model stopped (#42)', () => {
  /**
   * Anything but `tool_use` was returned as the answer: a turn cut off
   * at its length limit, a refusal, a paused turn. Each is said for
   * what it is now, and only `tool_use` runs tools.
   */
  it('says when an answer was cut off, and runs none of its tools', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm1', stop_reason: 'max_tokens', usage: USAGE,
        content: [
          { type: 'text', text: 'Counting the' },
          { type: 'tool_use', id: 'tu_1', name: 'dependents_of', input: { name: 'ma' } },
        ],
      }),
      answer('next answer'),
    );
    const { dataset, calls } = fakeDataset();
    const agent = new Agent(dataset);
    await expect(agent.ask('who declares mail?')).rejects.toThrow(/cut off/i);
    expect(calls).toEqual([]);

    await agent.ask('next');
    expect(posted(fetchMock, 1)).toEqual([{ role: 'user', content: 'next' }]);
  });

  it('says when the model declined, rather than showing what it had written', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm1', stop_reason: 'refusal', usage: USAGE,
        content: [{ type: 'text', text: 'Here is how to' }],
      }),
      answer('next answer'),
    );
    const agent = new Agent(fakeDataset().dataset);
    const failure = agent.ask('something declined');
    await expect(failure).rejects.toThrow(/declined/i);
    await expect(failure).rejects.not.toThrow(/Here is how to/);

    await agent.ask('next');
    expect(posted(fetchMock, 1)).toEqual([{ role: 'user', content: 'next' }]);
  });

  it('continues a paused turn by sending it back as it came', async () => {
    const paused = [{ type: 'text', text: 'Still counting.' }];
    const fetchMock = stubTurns(
      turn({ id: 'm1', stop_reason: 'pause_turn', usage: USAGE, content: paused }),
      answer('17 projects declare it.'),
    );
    const pauses = vi.fn();
    const agent = new Agent(fakeDataset().dataset, { onPause: pauses });

    await expect(agent.ask('who declares mail?')).resolves.toBe('17 projects declare it.');
    expect(pauses).toHaveBeenCalledTimes(1);
    // The paused turn, verbatim and last: no user message is added to
    // ask the model to go on.
    expect(posted(fetchMock, 1)).toEqual([
      { role: 'user', content: 'who declares mail?' },
      { role: 'assistant', content: paused },
    ]);
  });

  it('says when the conversation outgrew the model', async () => {
    stubTurns(
      turn({ id: 'm1', stop_reason: 'model_context_window_exceeded', usage: USAGE, content: [] }),
    );
    await expect(new Agent(fakeDataset().dataset).ask('q')).rejects.toThrow(
      /new conversation/i,
    );
  });

  it('names a reason it does not know, rather than taking it for an answer', async () => {
    stubTurns(
      turn({ id: 'm1', stop_reason: 'something_new', usage: USAGE, content: [{ type: 'text', text: 'x' }] }),
    );
    await expect(new Agent(fakeDataset().dataset).ask('q')).rejects.toThrow(/something_new/);
  });
});

describe('Agent: human verification (#32)', () => {
  /**
   * `setTurnstileToken` had no callers, and a token it was given would
   * have been resent on every turn, though Cloudflare accepts a token
   * once. The agent now asks the Worker what a question needs, solves
   * the challenge before the question's first turn, and presents the
   * session that turn is answered with on the turns after it.
   */
  const SITE_KEY = '0x4AAAAAAA-the-site-key';

  interface Posted {
    messages: unknown[];
    turnstileToken?: string;
    session?: string;
  }

  /**
   * The Worker: a GET answers what a question needs, each POST the next
   * turn. Functions, because a Response can be read only once.
   */
  function stubWorker(settings: () => Response, ...turns: Array<() => Response>) {
    const posted: Posted[] = [];
    const log: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        if ((init?.method ?? 'GET') === 'GET') {
          log.push('settings');
          return settings();
        }
        posted.push(JSON.parse(String(init?.body)) as Posted);
        log.push('turn');
        const next = turns.shift();
        if (!next) throw new Error('the conversation went on longer than the test');
        return next();
      }),
    );
    return { posted, log };
  }

  const required = () => turn({ turnstile: { siteKey: SITE_KEY } });

  const callsATool = (session?: string) => () =>
    turn({
      id: 'm1', stop_reason: 'tool_use', usage: USAGE,
      content: [{ type: 'tool_use', id: 'tu_1', name: 'language_coverage', input: {} }],
      ...(session ? { session } : {}),
    });

  const answers = (text: string, session?: string) => () =>
    turn({
      id: 'm2', stop_reason: 'end_turn', usage: USAGE,
      content: [{ type: 'text', text }],
      ...(session ? { session } : {}),
    });

  /** Hands out these tokens in turn, as a widget solving each challenge would. */
  function solver(...tokens: string[]) {
    return vi.fn(async (_siteKey: string) => {
      const token = tokens.shift();
      if (!token) throw new Error('asked for more tokens than the test has');
      return token;
    });
  }

  const tokensAndSessions = (posted: Posted[]) =>
    posted.map((body) => [body.turnstileToken, body.session]);

  it('solves the challenge before the first turn, and presents the session after it', async () => {
    const { posted, log } = stubWorker(required, callsATool('session-1'), answers('done'));
    const solve = vi.fn(async (siteKey: string) => {
      log.push(`solve ${siteKey}`);
      return 'token-1';
    });

    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solve);
    await expect(agent.ask('q')).resolves.toBe('done');

    expect(log).toEqual(['settings', `solve ${SITE_KEY}`, 'turn', 'turn']);
    expect(tokensAndSessions(posted)).toEqual([
      ['token-1', undefined],
      [undefined, 'session-1'],
    ]);
  });

  it('solves it again for the next question, which the last session does not cover', async () => {
    const { posted, log } = stubWorker(
      required,
      answers('first', 'session-1'),
      answers('second', 'session-2'),
    );
    const solve = solver('token-1', 'token-2');
    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solve);

    await agent.ask('one');
    await agent.ask('two');

    expect(log).toEqual(['settings', 'turn', 'settings', 'turn']);
    expect(tokensAndSessions(posted)).toEqual([
      ['token-1', undefined],
      ['token-2', undefined],
    ]);
  });

  it('solves nothing and sends neither when the Worker needs neither', async () => {
    const { posted } = stubWorker(
      () => turn({ turnstile: null }),
      callsATool(),
      answers('done'),
    );
    const solve = solver();

    await new Agent(fakeDataset().dataset, {}, '/api/chat', solve).ask('q');

    expect(solve).not.toHaveBeenCalled();
    expect(tokensAndSessions(posted)).toEqual([
      [undefined, undefined],
      [undefined, undefined],
    ]);
  });

  it('passes again when a session lapses mid-question, and retries that turn once', async () => {
    const { posted } = stubWorker(
      required,
      callsATool('session-1'),
      () =>
        turn(
          { error: 'Human verification has expired.', turnstile: { siteKey: SITE_KEY } },
          403,
        ),
      answers('done', 'session-2'),
    );
    const solve = solver('token-1', 'token-2');

    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solve);
    await expect(agent.ask('q')).resolves.toBe('done');

    expect(tokensAndSessions(posted)).toEqual([
      ['token-1', undefined],
      [undefined, 'session-1'],
      ['token-2', undefined],
    ]);
    // The same turn again: nothing was added to the conversation.
    expect(posted[2]!.messages).toEqual(posted[1]!.messages);
  });

  it('does not retry a turn whose fresh token was turned down', async () => {
    stubWorker(required, () =>
      turn(
        { error: 'Human verification failed. Reload and retry.', turnstile: { siteKey: SITE_KEY } },
        403,
      ),
    );
    const solve = solver('token-1', 'token-2');

    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solve);
    await expect(agent.ask('q')).rejects.toThrow(/verification failed/);
    expect(solve).toHaveBeenCalledTimes(1);
  });

  it('posts nothing when the challenge cannot be solved', async () => {
    const { posted } = stubWorker(required);
    const solve = vi.fn(async (_siteKey: string): Promise<string> => {
      throw new Error('The human verification check could not be loaded.');
    });

    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solve);
    await expect(agent.ask('q')).rejects.toThrow(/could not be loaded/);
    expect(posted).toHaveLength(0);
  });

  it('says so when the deployment has no AI answers to give', async () => {
    stubWorker(() =>
      turn({ error: 'AI answers are not configured on this deployment.' }, 503),
    );
    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solver());
    await expect(agent.ask('q')).rejects.toThrow(/not configured/);
  });

  it('drops a question whose session was refused after its one retry (#42)', async () => {
    const { posted } = stubWorker(
      required,
      callsATool('session-1'),
      () =>
        turn(
          { error: 'Human verification has expired.', turnstile: { siteKey: SITE_KEY } },
          403,
        ),
      () =>
        turn(
          { error: 'Human verification failed. Reload and retry.', turnstile: { siteKey: SITE_KEY } },
          403,
        ),
      answers('done', 'session-3'),
    );
    const solve = solver('token-1', 'token-2', 'token-3');
    const agent = new Agent(fakeDataset().dataset, {}, '/api/chat', solve);

    await expect(agent.ask('first')).rejects.toThrow(/verification failed/);
    await expect(agent.ask('second')).resolves.toBe('done');
    expect(posted.at(-1)!.messages).toEqual([{ role: 'user', content: 'second' }]);
  });
});
