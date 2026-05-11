import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { OfflineQueue, MemoryQueueStorage } from '../dist/client/index.js';
import { IdempotencyStore, MemoryIdempotencyBackend, MutationLog } from '../dist/server/index.js';

function buildSender() {
  const store = new IdempotencyStore(new MemoryIdempotencyBackend());
  const log = new MutationLog();
  const handlerCalls = new Map();
  const sender = async (batch) => {
    const results = [];
    for (const m of batch) {
      const res = await store.process(
        m.idempotencyKey,
        async () => {
          handlerCalls.set(m.idempotencyKey, (handlerCalls.get(m.idempotencyKey) ?? 0) + 1);
          return { applied: m.payload };
        },
        () => log.appendAndAssign({
          idempotencyKey: m.idempotencyKey,
          duplicate: false,
          result: null,
          serverTs: Date.now(),
        })
      );
      results.push(res);
    }
    return results;
  };
  return { sender, handlerCalls, log };
}

describe('OfflineQueue', () => {
  it('flushes pending mutations on enqueue and resolves with seqIds', async () => {
    const { sender, handlerCalls, log } = buildSender();
    const committed = [];
    const queue = new OfflineQueue({
      storage: new MemoryQueueStorage(),
      sender,
      initialBackoffMs: 1,
      onCommit: (m, r) => committed.push({ key: m.idempotencyKey, seqId: r.seqId }),
    });

    await queue.enqueue('admit', { name: 'Sarah' });
    await queue.enqueue('admit', { name: 'Alex' });
    await queue.flush();

    assert.equal(committed.length, 2);
    assert.equal(committed[0].seqId, 1);
    assert.equal(committed[1].seqId, 2);
    assert.equal(handlerCalls.size, 2);
    assert.equal(await queue.size(), 0);
    assert.equal(await log.head(), 2);
  });

  it('retries on transport failure without re-running the server handler', async () => {
    const { sender: innerSender, handlerCalls } = buildSender();
    let throws = 2;
    const sender = async (batch) => {
      if (throws > 0) {
        throws--;
        throw new Error('network');
      }
      return innerSender(batch);
    };

    const queue = new OfflineQueue({
      storage: new MemoryQueueStorage(),
      sender,
      initialBackoffMs: 1,
    });
    await queue.enqueue('admit', { name: 'Sarah' });
    await queue.flush().catch(() => {});
    await new Promise((r) => setTimeout(r, 30));
    await queue.flush().catch(() => {});
    await new Promise((r) => setTimeout(r, 30));
    await queue.flush();

    assert.equal([...handlerCalls.values()][0], 1);
    assert.equal(await queue.size(), 0);
  });

  it('sending the same mutation twice (e.g. accidental double-flush) commits once', async () => {
    const { sender, handlerCalls } = buildSender();
    const queue = new OfflineQueue({
      storage: new MemoryQueueStorage(),
      sender,
      initialBackoffMs: 1,
    });
    const key = await queue.enqueue('admit', { name: 'Sarah' });
    await queue.flush();
    await queue.flush();
    await queue.flush();
    assert.equal(handlerCalls.get(key), 1, 'server should see the mutation exactly once');
  });
});
