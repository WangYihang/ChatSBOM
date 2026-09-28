/** A stand-in for the Messages API, on this machine (standin.mjs). */
export interface StandIn {
  /** Start listening; resolves with its base URL. */
  listen(): Promise<string>;
  /** How many calls it has taken. */
  calls(): number;
  /** Let every answer held so far, and every later one, go. */
  release(): void;
  close(): Promise<void>;
}

export function standIn(): StandIn;
