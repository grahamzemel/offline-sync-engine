import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { IdempotencyStore, MemoryIdempotencyBackend, MutationLog } from '../dist/server/index.js';

describe('IdempotencyStore', () => {
  it('runs the handler exactly once even with repeated keys', async () => {
    const store = new IdempotencyStore(new MemoryIdempotencyBackend());
    const log = new MutationLog();
    let calls = 0;
    const handler = async () => {
      calls++;
      return { admitted: true };
    };
    const seq = () => log.appendAndAssign({
      idempotencyKey: 'k1',
      duplicate: false,
      result: { admitted: true },
      serverTs: Date.now(),
    });

    const r1 = await store.process('k1', handler, seq);
    const r2 = await store.process('k1', handler, seq);
    const r3 = await store.process('k1', handler, seq);

    assert.equal(calls, 1, 'handler should only run once');
    assert.equal(r1.duplicate, false);
    assert.equal(r2.duplicate, true);
    assert.equal(r3.duplicate, true);
    assert.equal(r1.seqId, r2.seqId);
    assert.equal(r2.seqId, r3.seqId);
    assert.deepEqual(r2.result, { admitted: true });
  });

  it('releases reservation on handler failure so retries succeed', async () => {
    const store = new IdempotencyStore(new MemoryIdempotencyBackend());
    const log = new MutationLog();
    let attempts = 0;
    const handler = async () => {
      attempts++;
      if (attempts === 1) throw new Error('boom');
      return { ok: true };
    };
    const seq = () => log.appendAndAssign({
      idempotencyKey: 'k2',
      duplicate: false,
      result: { ok: true },
      serverTs: Date.now(),
    });

    await assert.rejects(() => store.process('k2', handler, seq), /boom/);
    const r2 = await store.process('k2', handler, seq);
    assert.equal(attempts, 2);
    assert.equal(r2.duplicate, false);
    assert.equal(r2.seqId, 1);
  });

  it('different keys do not interfere with each other', async () => {
    const store = new IdempotencyStore(new MemoryIdempotencyBackend());
    const log = new MutationLog();
    const seq = () => log.appendAndAssign({
      idempotencyKey: 'irrelevant',
      duplicate: false,
      result: {},
      serverTs: Date.now(),
    });
    const r1 = await store.process('a', async () => 1, seq);
    const r2 = await store.process('b', async () => 2, seq);
    assert.equal(r1.result, 1);
    assert.equal(r2.result, 2);
    assert.notEqual(r1.seqId, r2.seqId);
  });
});

describe('MutationLog catch-up', () => {
  it('returns events strictly after sinceSeqId in order', async () => {
    const log = new MutationLog();
    for (let i = 0; i < 5; i++) {
      await log.appendAndAssign({
        idempotencyKey: `k${i}`,
        duplicate: false,
        result: { i },
        serverTs: Date.now(),
      });
    }
    const after2 = await log.since(2);
    assert.equal(after2.length, 3);
    assert.equal(after2[0].seqId, 3);
    assert.equal(after2[0].result.i, 2);
    assert.equal(after2[2].seqId, 5);
    assert.equal(await log.head(), 5);
  });

  it('returns empty array when no new events', async () => {
    const log = new MutationLog();
    await log.appendAndAssign({
      idempotencyKey: 'k1',
      duplicate: false,
      result: {},
      serverTs: Date.now(),
    });
    assert.equal((await log.since(5)).length, 0);
  });
});
