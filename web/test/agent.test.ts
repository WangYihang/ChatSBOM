import { describe, expect, it, vi } from 'vitest';

import { Agent, AgentError } from '../src/agent';
import type { Dataset } from '../src/queries';

/** A Dataset stub that records which tools the agent actually ran. */
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
  } as unknown as Dataset;
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

    // The second request must carry the tool_result keyed to tu_1.
    const second = JSON.parse(fetchMock.mock.calls[1][1].body);
    const results = second.messages.at(-1).content;
    expect(results[0]).toMatchObject({
      type: 'tool_result', tool_use_id: 'tu_1',
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
    const second = JSON.parse(fetchMock.mock.calls[1][1].body);
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

    const second = JSON.parse(fetchMock.mock.calls[1][1].body);
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

  it('sends the Turnstile token when it has one', async () => {
    const fetchMock = stubTurns(
      turn({
        id: 'm', stop_reason: 'end_turn', usage: USAGE,
        content: [{ type: 'text', text: 'ok' }],
      }),
    );
    const { dataset } = fakeDataset();
    const agent = new Agent(dataset, {}, '/api/chat');
    agent.setTurnstileToken('tok');
    await agent.ask('q');

    expect(JSON.parse(fetchMock.mock.calls[0][1].body).turnstileToken)
      .toBe('tok');
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

    const second = JSON.parse(fetchMock.mock.calls[1][1].body);
    expect(second.messages).toHaveLength(3);
    expect(second.messages[0]).toMatchObject({ role: 'user', content: 'first' });
  });
});
