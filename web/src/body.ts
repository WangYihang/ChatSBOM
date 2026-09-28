/**
 * A request body, read against a hard cap on the bytes that arrive.
 *
 * Shared by both endpoints. It was the chat's alone, and the query
 * endpoint read whatever came with `request.json()` (#31).
 */

/** A body refused before it was parsed: too large, or not readable at all. */
export class BodyError extends Error {
  constructor(
    readonly status: 400 | 413,
    message: string,
  ) {
    super(message);
  }
}

/**
 * The body as text, or a `BodyError`.
 *
 * `Content-Length` is a claim, and a chunked request makes none:
 * `request.json()` read whatever came, and a 2 MiB body went through.
 * The claim is checked first because it is free; counting while reading
 * then holds the cap however the body is framed, and stops at the chunk
 * that crosses it instead of buffering the rest.
 */
export async function readBody(request: Request, maxBytes: number): Promise<string> {
  if (Number(request.headers.get('content-length') ?? '0') > maxBytes) {
    throw new BodyError(413, 'Request too large.');
  }
  if (!request.body) return '';
  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let received = 0;
  let text = '';
  for (;;) {
    // A client that hangs up mid-upload is its problem, not a failure here.
    const { done, value } = await reader.read().catch(() => {
      throw new BodyError(400, 'The body could not be read.');
    });
    if (done) return text + decoder.decode();
    received += value.byteLength;
    if (received > maxBytes) {
      await reader.cancel();
      throw new BodyError(413, 'Request too large.');
    }
    text += decoder.decode(value, { stream: true });
  }
}
