/**
 * Express example wiring up offline-sync-engine end-to-end.
 *
 * Three endpoints:
 *   POST /mutations   accept a batch, dedupe with idempotency keys, return CommitResults
 *   GET  /sync        catch-up: return everything since `?since=NN`
 *   GET  /events/:topic  SSE stream for realtime fanout
 *
 * Plug your own DB into the `handler` callback below — the library only
 * cares about the contract, not what your domain mutations actually do.
 */
import express from 'express';
import {
  IdempotencyStore,
  MemoryIdempotencyBackend,
  MutationLog,
  Broadcaster,
} from 'offline-sync-engine/server';
import type { Mutation } from 'offline-sync-engine/server';

const app = express();
app.use(express.json());

const store = new IdempotencyStore(new MemoryIdempotencyBackend());
const log = new MutationLog();
const broadcaster = new Broadcaster();

// Domain handlers — swap with your actual DB writes.
type Handler = (payload: any) => Promise<unknown>;
const handlers: Record<string, Handler> = {
  admit: async (payload) => {
    // e.g. await db.insert('admits', payload);
    return { admittedAt: Date.now(), ...payload };
  },
};

app.post('/mutations', async (req, res) => {
  const batch: Mutation[] = req.body?.mutations ?? [];
  const topic = String(req.body?.topic || 'default');
  const results = [];
  for (const m of batch) {
    const handler = handlers[m.type];
    if (!handler) {
      results.push({
        idempotencyKey: m.idempotencyKey,
        seqId: -1,
        duplicate: false,
        result: { error: `unknown mutation type: ${m.type}` },
        serverTs: Date.now(),
      });
      continue;
    }
    // Announce so other scanners see "pending" optimistically.
    broadcaster.announce(topic, m.idempotencyKey, m.type, m.clientId);
    const commit = await store.process(
      m.idempotencyKey,
      () => handler(m.payload),
      () =>
        log.appendAndAssign({
          idempotencyKey: m.idempotencyKey,
          duplicate: false,
          result: null, // overwritten on next line
          serverTs: Date.now(),
        })
    );
    broadcaster.commit(topic, commit);
    results.push(commit);
  }
  res.json({ results });
});

app.get('/sync', async (req, res) => {
  const since = Number(req.query.since ?? 0);
  const events = await log.since(since, 1000);
  const headSeqId = await log.head();
  res.json({
    events: events.map((c) => ({ kind: 'commit' as const, ...c })),
    headSeqId,
  });
});

app.get('/events/:topic', (req, res) => {
  broadcaster.subscribe(req.params.topic, res as any);
});

app.listen(3000, () => console.log('Listening on http://localhost:3000'));
