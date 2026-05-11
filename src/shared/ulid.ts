/**
 * Minimal dependency-free ULID generator.
 *
 * ULIDs are lexicographically sortable by creation time and 128 bits wide,
 * so collisions are effectively impossible across all clients in your fleet.
 * That's what makes them perfect for idempotency keys: the client can generate
 * one for every mutation without coordinating with the server, and the server
 * can use them as document IDs without fear of collision.
 *
 * Format: 26 chars, Crockford base32. First 10 chars encode the timestamp
 * (millisecond precision, sortable), last 16 chars are random.
 *
 * Spec: https://github.com/ulid/spec
 */
const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
const ENCODING_LEN = ENCODING.length;
const TIME_LEN = 10;
const RANDOM_LEN = 16;

function getRandomBytes(n: number): Uint8Array {
  // Browser path
  if (typeof globalThis.crypto !== 'undefined' && globalThis.crypto.getRandomValues) {
    const arr = new Uint8Array(n);
    globalThis.crypto.getRandomValues(arr);
    return arr;
  }
  // Node path
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const nodeCrypto = require('node:crypto');
  return new Uint8Array(nodeCrypto.randomBytes(n));
}

function encodeTime(now: number): string {
  let time = now;
  const out = new Array<string>(TIME_LEN);
  for (let i = TIME_LEN - 1; i >= 0; i--) {
    const mod = time % ENCODING_LEN;
    out[i] = ENCODING[mod];
    time = (time - mod) / ENCODING_LEN;
  }
  return out.join('');
}

function encodeRandom(): string {
  const bytes = getRandomBytes(RANDOM_LEN);
  let out = '';
  for (let i = 0; i < RANDOM_LEN; i++) {
    out += ENCODING[bytes[i] % ENCODING_LEN];
  }
  return out;
}

export function ulid(now: number = Date.now()): string {
  return encodeTime(now) + encodeRandom();
}

/** Extract the millisecond timestamp from a ULID. */
export function ulidTimestamp(id: string): number {
  let ms = 0;
  for (let i = 0; i < TIME_LEN; i++) {
    const idx = ENCODING.indexOf(id[i]);
    if (idx < 0) throw new Error(`Invalid ULID: ${id}`);
    ms = ms * ENCODING_LEN + idx;
  }
  return ms;
}
