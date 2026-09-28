// A stand-in for the Messages API, on this machine, for a Worker run in
// workerd to send its model calls to (spend.integration.test.ts).
//
// JavaScript rather than TypeScript: it is Node's HTTP server, and the
// tests are typed against the Workers runtime and the DOM, not Node.
// Its shape is declared beside it, in standin.d.mts.
import http from 'node:http';

/** Answers every call alike, holding each answer until released. */
export function standIn() {
  let calls = 0;
  let release = () => {};
  const released = new Promise((resolve) => (release = resolve));
  const server = http.createServer((request, response) => {
    request.resume();
    request.on('end', async () => {
      calls += 1;
      await released;
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(
        JSON.stringify({
          id: `msg_${calls}`,
          type: 'message',
          role: 'assistant',
          model: 'claude-opus-5',
          stop_reason: 'end_turn',
          stop_sequence: null,
          content: [{ type: 'text', text: 'A stand-in answer.', citations: null }],
          usage: { input_tokens: 1_000, output_tokens: 100 },
        }),
      );
    });
  });
  return {
    listen: () =>
      new Promise((resolve) =>
        server.listen(0, '127.0.0.1', () => {
          resolve(`http://127.0.0.1:${server.address().port}`);
        }),
      ),
    calls: () => calls,
    release: () => release(),
    close: () => new Promise((resolve) => server.close(() => resolve())),
  };
}
