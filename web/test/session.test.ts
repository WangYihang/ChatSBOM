/**
 * The session a verified question's later turns present (#32).
 *
 * Cloudflare accepts a Turnstile token once, and a question is several
 * turns. What stands in for the token after the first turn has to be
 * as hard to come by for anything but that question, from that client,
 * for a short while — or it is a way around Turnstile rather than a
 * way through it.
 */
import type Anthropic from '@anthropic-ai/sdk';
import { describe, expect, it } from 'vitest';

import { checkSession, issueSession, sessionScope } from '../src/session';

const SECRET = 'the-turnstile-secret';
const NOW = new Date('2026-09-14T10:00:00Z');
const CLIENT = '203.0.113.7';

const at = (seconds: number) => new Date(NOW.getTime() + seconds * 1000);

const question: Anthropic.MessageParam = { role: 'user', content: 'who declares mail?' };
const toolCall: Anthropic.MessageParam = {
  role: 'assistant',
  content: [{ type: 'tool_use', id: 'toolu_01', name: 'ecosystems_for', input: { name: 'mail' } }],
};
const toolResult: Anthropic.MessageParam = {
  role: 'user',
  content: [{ type: 'tool_result', tool_use_id: 'toolu_01', content: '[]' }],
};
const answer: Anthropic.MessageParam = {
  role: 'assistant',
  content: [{ type: 'text', text: '17 projects declare it.' }],
};
const nextQuestion: Anthropic.MessageParam = { role: 'user', content: 'and on maven?' };

describe('what a session is bound to', () => {
  it('is the same for every turn of one question', async () => {
    const first = await sessionScope([question], CLIENT);
    expect(first).not.toBeNull();
    expect(await sessionScope([question, toolCall, toolResult], CLIENT)).toBe(first);
  });

  it('differs for the next question, and for the same words asked elsewhere', async () => {
    const scopes = await Promise.all([
      sessionScope([question], CLIENT),
      sessionScope([question, answer, nextQuestion], CLIENT),
      sessionScope([nextQuestion, answer, question], CLIENT),
    ]);
    expect(new Set(scopes).size).toBe(3);
  });

  it('differs by client', async () => {
    expect(await sessionScope([question], CLIENT)).not.toBe(
      await sessionScope([question], '198.51.100.9'),
    );
  });

  it('does not exist for a conversation with no question in it', async () => {
    expect(await sessionScope([toolCall, toolResult], CLIENT)).toBeNull();
  });
});

describe('a session', () => {
  it('is good for its own question, for ten minutes', async () => {
    const scope = (await sessionScope([question], CLIENT))!;
    const token = await issueSession(SECRET, scope, NOW);

    expect(await checkSession(SECRET, token, scope, at(0))).toBe(true);
    expect(await checkSession(SECRET, token, scope, at(599))).toBe(true);
    expect(await checkSession(SECRET, token, scope, at(600))).toBe(false);
  });

  it('is good for nothing else', async () => {
    const scope = (await sessionScope([question], CLIENT))!;
    const other = (await sessionScope([question, answer, nextQuestion], CLIENT))!;
    const token = await issueSession(SECRET, scope, NOW);

    expect(await checkSession(SECRET, token, other, NOW)).toBe(false);
    expect(await checkSession('another-secret', token, scope, NOW)).toBe(false);
  });

  it.each(['', 'nonsense', '1.2', `99999999999.${'0'.repeat(64)}`, `-1.${'0'.repeat(64)}`])(
    'refuses %j without throwing',
    async (token) => {
      const scope = (await sessionScope([question], CLIENT))!;
      await expect(checkSession(SECRET, token, scope, NOW)).resolves.toBe(false);
    },
  );
});
