import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ulid, ulidTimestamp } from '../dist/shared/ulid.js';

describe('ulid', () => {
  it('produces 26-char Crockford base32 strings', () => {
    const id = ulid();
    assert.equal(id.length, 26);
    assert.match(id, /^[0-9A-HJKMNP-TV-Z]{26}$/);
  });

  it('is monotonically sortable by creation time at the millisecond level', () => {
    const t1 = ulid(1_700_000_000_000);
    const t2 = ulid(1_700_000_001_000);
    const t3 = ulid(1_800_000_000_000);
    assert.ok(t1 < t2);
    assert.ok(t2 < t3);
  });

  it('round-trips timestamp via ulidTimestamp', () => {
    const now = Date.now();
    const id = ulid(now);
    assert.equal(ulidTimestamp(id), now);
  });

  it('generates collision-free ids in tight loops (1000 calls)', () => {
    const seen = new Set();
    for (let i = 0; i < 1000; i++) seen.add(ulid());
    assert.equal(seen.size, 1000);
  });
});
