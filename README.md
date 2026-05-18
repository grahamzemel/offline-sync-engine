# offline-sync-engine

**Keep your check-in scanner working when the venue's WiFi dies.**

A tiny offline-first mutation queue + idempotent server primitives. About 900 lines. Zero runtime dependencies.

```
npm install offline-sync-engine
```

[![npm](https://img.shields.io/npm/v/offline-sync-engine)](https://www.npmjs.com/package/offline-sync-engine)
[![downloads](https://img.shields.io/npm/dm/offline-sync-engine)](https://npm-stat.com/charts.html?package=offline-sync-engine)
[![CI](https://github.com/grahamzemel/offline-sync-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/grahamzemel/offline-sync-engine/actions/workflows/ci.yml)

🎯 **[Live demo](https://grahamzemel.github.io/offline-sync-engine/)** · 📈 **[Download trends](https://npm-stat.com/charts.html?package=offline-sync-engine)**

## Is this for you?

Use this if **all** of these are true:

- [ ] You have a web or mobile-web app that performs user-initiated mutations (check-ins, orders, votes, form saves, attendance, etc.).
- [ ] Some of your users are on flaky / intermittent networks (event venues, warehouses, in-store, vehicles, rural).
- [ ] Losing a mutation or accidentally double-applying one would be bad (refunds, duplicate admits, missing orders).
- [ ] You control the backend (Node/Express today; any HTTP server if you port the ~30-line server primitives).
- [ ] Your client runs in a modern browser with IndexedDB (basically everything since ~2017).

If you only need read-cache offline (showing data when offline) and don't have writes, you don't need this — a service worker is enough.

## Step-by-step setup (~15 min)

> Stuck on any step? Paste this README + your existing fetch call + route handler into [Claude.ai](https://claude.ai) and ask it to wire the library into your code. It works well for this.

1. **Install.**
   ```
   npm install offline-sync-engine
   ```
2. **Server — add the idempotency store and a POST endpoint** (see [Server example](#server-express) below). Pick `MemoryIdempotencyBackend` to start; swap in Redis/Postgres/Firestore later.
3. **Server — allow the idempotency header in CORS.** This is the #1 thing people miss. See [CORS section](#-required-allow-the-idempotency-header-in-your-cors-config) below.
4. **Client — open IndexedDB storage and create the queue** (see [Client example](#client) below).
5. **Replace your existing `fetch('/admits', ...)` call with `queue.enqueue('admit', payload)`.** That's the whole behavioral change in your scanner / form / order code.
6. **Test offline.** In Chrome DevTools → Network → set to "Offline." Perform a mutation. You should see it queued (UI updates immediately). Switch back to "Online." Watch it flush in the Network tab — you'll see one batched POST with your mutation(s).
7. **(Optional) Add multi-device sync.** Only if you have multiple scanners/devices that need realtime coordination — see [the optional section](#optional-multi-scanner-coordination).


## The problem this solves

You're running door admits for an event. The venue's WiFi is flaky. A typical "naive" offline implementation has three failure modes:

1. **Lost admits** — when offline, the scan request fails and the admit is dropped on the floor.
2. **Phantom duplicates on reconnect** — admit succeeds on the server but the ack gets lost, so the client retries. Server inserts a second row. Now you've admitted the same person twice.
3. **Lost on reload** — if the in-memory retry array is the only place storing pending admits, refreshing the tab nukes everything queued.

This library fixes all three with four small primitives. The main one is `OfflineQueue` — your scanner code calls `queue.enqueue('admit', payload)` and the rest is taken care of: ULID idempotency key, IndexedDB persistence, exponential-backoff retry, exactly-once delivery to the server.

The "two scanners racing" / realtime-sync features (`Broadcaster`, `LiveSync`, `MutationLog`) are optional add-ons for when you have multiple devices that need to coordinate. Most apps just need the single-scanner reliability piece.

## The primitives

| Primitive | What it gives you | Required? |
|---|---|---|
| **`OfflineQueue`** (client) | Persistent IndexedDB queue. Each mutation tagged with a ULID idempotency key at creation time. Auto-flushes on reconnect with exponential backoff. | **Yes — this is the core piece.** |
| **`IdempotencyStore`** (server) | Reserves a key, runs your handler, caches the result. Retries of the same key return the cached commit instead of re-running. | **Yes** — pair with the queue. |
| **`MutationLog`** (server) | Append-only log assigning a monotonic `seqId` to every commit. | Optional — only needed if you want catch-up replay. |
| **`Broadcaster`** + **`LiveSync`** | SSE channel for realtime fanout with auto catch-up on reconnect. | Optional — only needed for multi-device coordination. |

## Quick start — single scanner

This is the 80% case. One device, one user, takes admits offline and reliably syncs when WiFi returns.

### Client

```ts
import { OfflineQueue, IndexedDBQueueStorage } from 'offline-sync-engine/client';

const storage = await IndexedDBQueueStorage.open('checkin-app');

const queue = new OfflineQueue({
  storage,
  // Your batched POST. Throw on transport errors — the queue handles retry.
  sender: async (batch) => {
    const res = await fetch('/admits', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mutations: batch }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return (await res.json()).results;  // CommitResult[]
  },
  onCommit: (m, r) => {
    // Update your local UI's "admitted" set. r.seqId is the server's id.
  },
});

// Wire to your scan handler:
async function onScan(guest) {
  await queue.enqueue('admit', { guestId: guest.id, name: guest.name });
  // Returns the moment the mutation is in IndexedDB. UI updates instantly.
  // If you're offline, the queue holds it until you're back online.
}
```

That's it on the client. No "offline mode" branch in your scanner code — the queue is always between you and the network, so the call path is identical whether you're online, offline, or losing packets.

### Server (Express)

```ts
import { IdempotencyStore, MutationLog } from 'offline-sync-engine/server';

const store = new IdempotencyStore();    // swap backend for Redis/Postgres/Firestore in prod
const log = new MutationLog();

app.post('/admits', async (req, res) => {
  const results = [];
  for (const m of req.body.mutations) {
    const commit = await store.process(
      m.idempotencyKey,
      () => admitGuest(m.payload),  // your domain handler — runs at most once per key
      () => log.appendAndAssign({
        idempotencyKey: m.idempotencyKey,
        duplicate: false,
        result: null,
        serverTs: Date.now(),
      })
    );
    results.push(commit);
  }
  res.json({ results });
});
```

`store.process` is the magic: same idempotency key sent twice → handler runs once, second call returns the cached commit with `duplicate: true`. That kills the "ack-got-lost-so-retry-and-duplicate" failure mode at the root.

## ⚠️ Required: allow the idempotency header in your CORS config

If your client and server are on different origins (the common case — `app.yourdomain.com` calling `api.yourdomain.com`, or `localhost:5173` calling `localhost:3000`), the browser sends a CORS **preflight** for every POST that carries a custom header. If the server doesn't explicitly whitelist `X-Idempotency-Key` (and `X-Offline-Sync-Engine` if you set it), every single request is blocked with:

```
Access to fetch at '...' has been blocked by CORS policy: Request header field
x-idempotency-key is not allowed by Access-Control-Allow-Headers in preflight response.
```

This is the #1 thing people miss when wiring this library into an existing API. Two ways to fix it:

### With the `cors` middleware

```js
import cors from 'cors';

app.use(cors({
  origin: ['https://app.example.com', 'http://localhost:5173'],
  credentials: true,
  allowedHeaders: [
    'Content-Type',
    'Authorization',
    'X-Idempotency-Key',        // ← required
    'X-Offline-Sync-Engine',    // ← optional, useful for logging
  ],
}));
```

### With manual CORS headers

```js
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', req.headers.origin);
  res.header('Access-Control-Allow-Credentials', 'true');
  res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.header(
    'Access-Control-Allow-Headers',
    'Origin, X-Requested-With, Content-Type, Accept, Authorization, X-Idempotency-Key, X-Offline-Sync-Engine'
  );
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});
```

After the change, restart/redeploy your server. On the client side you'll see your queue start delivering successfully; without it, every send fails with `net::ERR_FAILED` (preflight rejection) and the library queue just keeps growing.

If you only ever set the idempotency key on `same-origin` requests (e.g., reverse-proxied API behind the same domain), preflight is skipped and you don't need to touch CORS. But if `fetch()` shows the origin and host don't match, you need this.

## Optional: multi-scanner coordination

If you have multiple devices, layer on `Broadcaster` + `LiveSync` for realtime fanout. Other scanners see your admits within ~50ms, and `LiveSync` auto-replays any events missed during a WiFi blip via the `MutationLog` catchup endpoint.

```ts
// Server
const broadcaster = new Broadcaster();
app.get('/events/:topic', (req, res) => broadcaster.subscribe(req.params.topic, res));
app.get('/sync', async (req, res) => {
  const since = Number(req.query.since ?? 0);
  res.json({
    events: (await log.since(since)).map((c) => ({ kind: 'commit', ...c })),
    headSeqId: await log.head(),
  });
});
// After every commit, broadcaster.commit(topic, commit);

// Client
import { LiveSync } from 'offline-sync-engine/client';
const live = new LiveSync({
  url: '/events/door',
  catchupUrl: (since) => `/sync?since=${since}`,
  onCommit: (e) => updatePeerView(e.result),
});
live.start();
```

Skip this entirely if you have one scanner per door. It's strictly additive.

## Why each piece exists

### ULID idempotency keys

Each mutation gets a 26-character lexicographically-sortable ULID at the moment it's created on the client. **This is the cornerstone of the whole system.** It means:

- The client can safely retry the same mutation forever without the server running it twice.
- The same key survives reload, restart, and tab switch — it lives in IndexedDB.
- Two scanners can't accidentally produce colliding keys (128 bits of entropy).
- Logs are time-sortable: you can scan the keys to see the order of intent.

The ULID generator is dependency-free and works in browser, Node, and edge runtimes.

### Persistent server-side idempotency store

The naive approach — an in-memory `Map` with a 30-second window — falls apart at three points:

1. **Server restart**: the map is empty, retries get re-processed.
2. **Horizontal scaling**: different pods have different maps, so the same key can run on two pods at once.
3. **Late retries**: a queued mutation from yesterday hits the server today, after the window expired.

`IdempotencyStore` solves all three by going through a `IdempotencyBackend` you implement. The library ships a `MemoryIdempotencyBackend` for tests and single-process apps. Production users write a 20-30 line `RedisIdempotencyBackend` or `FirestoreIdempotencyBackend` and inject it.

The contract is intentionally tiny:

```ts
interface IdempotencyBackend {
  reserveOrGet<R>(key: string): Promise<IdempotencyRecord<R> | null>;
  commit<R>(record: IdempotencyRecord<R>): Promise<void>;
  release(key: string): Promise<void>;
}
```

### Pending broadcasts (optimistic coordination)

When scanner A is *about to* admit Sarah, it announces a `pending` event before the POST finishes. Scanner B's `onPending` callback fires; B's local UI marks Sarah as "being admitted by A" and refuses to scan her again. If A's commit succeeds, B sees a `commit` event and finalizes. If A's commit fails or never arrives, B can time out the pending state.

This trades a few milliseconds of latency for correctness when two scanners race. Without it, both scanners are flying blind during the window between "started writing" and "wrote successfully." A pure server-side dedup can prevent the double-write but can't prevent the bad UX of both scanners briefly thinking they admitted Sarah.

### Catch-up via MutationLog

EventSource auto-reconnects, but it doesn't tell you what you missed. If scanner B's WiFi blips for 8 seconds and 3 admits happen during that window, the SSE channel just resumes — scanner B never sees those 3 events.

`MutationLog` solves this by assigning a monotonic `seqId` to every commit and serving a `GET /sync?since=N` endpoint that returns everything after seq N. `LiveSync` tracks the last seqId it has applied; on every SSE reconnect, it hits the catchup endpoint first, replays missed events, and only then resumes live updates.

This is the same pattern as Linear's sync engine and Replicache's pull endpoint, just simpler.

## What this isn't

- **Not a CRDT library.** If you need offline-merging text editors or whiteboards, use Yjs or Automerge.
- **Not a full sync engine.** If you need bidirectional model sync with mutators and pull/push semantics, use Replicache or Reflect.
- **Not a queue for general background jobs.** It assumes mutations are user-initiated, ordered loosely by time, and dedupable by key.

It's the *narrow* slice: durable client queue + idempotent server + log replay + realtime fanout. About 700 lines of code.

## Failure modes it handles

| Scenario | Behavior |
|---|---|
| Single scanner offline, queues 14 mutations | All 14 ship in a batch on reconnect. Server commits all 14, returns 14 seqIds. |
| Two scanners offline, both admit Sarah | Both queue locally with different ULIDs. On replay, server sees two distinct keys but the domain handler can detect "this guest is already admitted" and reject one with a domain error. The library doesn't try to be smarter than the domain. |
| Same scanner sends a batch, browser crashes mid-response | Queue persists. On restart, queue replays the same batch with same keys. Server returns cached `CommitResult`s with `duplicate: true`. No double-admits. |
| Scanner B's SSE drops for 8 seconds, scanner A admits 3 people | On reconnect, `LiveSync` calls `/sync?since=N`, replays the 3 commits, then resumes live stream. B is fully caught up. |
| Network gives 503 every other request | Queue retries with exponential backoff (500ms → 30s). Mutations sit in IndexedDB forever until ack'd. |
| Handler throws on the server | Reservation is released. Client retries, handler runs fresh. No phantom commits. |
| Server pod dies after writing commit but before sending response | Client retries. Server backend sees existing record (because `commit()` was already called), returns it as `duplicate: true`. Idempotent. |

## Roadmap

- Postgres `IdempotencyBackend` and `MutationLogBackend` reference implementations.
- Redis backends (SETNX-based reservations, Streams-based log).
- Firestore backend (using transactions for atomic reserve).
- Optional WebSocket transport in addition to SSE.
- Hook adapters for React (`useOfflineQueue`, `useLiveSync`).
- "Conflict policy" hook for the domain handler — pluggable resolver instead of just throwing.

## License

MIT
