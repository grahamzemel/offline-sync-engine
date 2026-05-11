# Migrating FratDoor to offline-sync-engine

The current FratDoor offline check-in code (in `website/src/components/dashboard/Dashboard.svelte` and `api/server.js`) is the inspiration for this library. Below is a concrete swap-in mapping. Each section shows the existing code and what to replace it with.

## What exists today (audited)

| Component | Today | Concern |
|---|---|---|
| Client queue | `openDB("fratdoor-offline", 1)` → `swipeQueue` store with auto-increment IDs | No client-generated UUID. Retries can produce dup rows server-side when ack is lost. |
| Client sender | Direct `fetch(/swipe)` for online; queued for offline, retried on `online` event | Naive retry: no per-mutation idempotency key, retries on transient errors potentially double-send. |
| Server dedup (online `/swipe`) | In-memory `_recentAdmissions` Map, 30-second window | Volatile; evaporates on pod restart; doesn't cross instances. |
| Server dedup (offline `/check-in`) | Firestore doc ID = `checkin_${user}_${name}_${tsMs}` | Works but uses name+timestamp instead of a true idempotency key; collides on homonyms. |
| Realtime to peer scanners | `broadcastToUser` over SSE | Doesn't catch up missed events on reconnect. |
| Order/seq tracking | None | Can't tell if a scanner is missing events. |

## The migration

### 1. Replace the IndexedDB queue (client)

**Before** — `Dashboard.svelte` around the existing `openDB("fratdoor-offline")` block:

```js
const offlineDb = await openDB("fratdoor-offline", 1, {
  upgrade(db) {
    db.createObjectStore("swipeQueue", { keyPath: "id", autoIncrement: true });
  },
});
// ...
await offlineDb.add("swipeQueue", {
  name,
  timestamp: Date.now(),
  user: username,
  isWhitelisted,
  isBlacklisted,
  rawInput,
  status,
  synced: false,
  retries: 0,
});
```

**After:**

```js
import {
  OfflineQueue,
  IndexedDBQueueStorage,
} from 'offline-sync-engine/client';

const storage = await IndexedDBQueueStorage.open('fratdoor-offline', 'swipeQueue');

const queue = new OfflineQueue({
  storage,
  clientId: deviceId,  // your existing _deviceId
  sender: async (batch) => {
    const res = await fetch(`${backendURL}/swipe/batch`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ mutations: batch }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    return body.results; // CommitResult[]
  },
  onCommit: (m, r) => {
    if (r.duplicate) console.log(`[swipe] ${m.idempotencyKey} was a duplicate — already admitted at seq ${r.seqId}`);
  },
});

// Every swipe call becomes:
await queue.enqueue('swipe', {
  fratUsername,
  fullName: name,
  rawInput,
  isWhitelisted,
  isBlacklisted,
  status,
});
```

The queue handles: persistence, ULID generation, batching, exponential backoff retry, online/offline detection.

### 2. Add idempotent batch endpoint (server)

**Before** — the existing two endpoints `/swipe` (online) and `/check-in` (offline replay) with two different dedup strategies.

**After** — one endpoint that handles both, with a real IdempotencyStore.

You'll write a thin Firestore adapter — the library only specifies the interface:

```js
// api/idempotencyBackendFirestore.js
const db = admin.firestore();

export const firestoreIdempotencyBackend = {
  async reserveOrGet(key) {
    const ref = db.collection('idempotency').doc(key);
    return db.runTransaction(async (txn) => {
      const snap = await txn.get(ref);
      if (snap.exists) {
        return snap.data();  // already committed — return the cached result
      }
      txn.set(ref, { reserved: true, reservedAt: admin.firestore.FieldValue.serverTimestamp() });
      return null;  // fresh reservation — caller should run the handler
    });
  },
  async commit(record) {
    await db.collection('idempotency').doc(record.idempotencyKey).set(record);
  },
  async release(key) {
    await db.collection('idempotency').doc(key).delete();
  },
};
```

Then the batch endpoint:

```js
import { IdempotencyStore, MutationLog, Broadcaster } from 'offline-sync-engine/server';
import { firestoreIdempotencyBackend } from './idempotencyBackendFirestore.js';

const store = new IdempotencyStore(firestoreIdempotencyBackend);
const log = new MutationLog(/* firestoreMutationLogBackend — similar adapter */);
const broadcaster = new Broadcaster();

app.post('/swipe/batch', checkJwt, async (req, res) => {
  const username = req.user.username;
  const results = [];
  for (const m of req.body.mutations) {
    broadcaster.announce(username, m.idempotencyKey, m.type, m.clientId);
    const commit = await store.process(
      m.idempotencyKey,
      async () => {
        // Wrap the existing swipe domain logic. Returns the same shape it
        // used to write to Firestore, but the IdempotencyStore guarantees
        // it runs at most once per key.
        return performSwipeWrite(username, m.payload);
      },
      () => log.appendAndAssign({
        idempotencyKey: m.idempotencyKey,
        duplicate: false,
        result: null,
        serverTs: Date.now(),
      })
    );
    broadcaster.commit(username, commit);
    results.push(commit);
  }
  res.json({ results });
});
```

This collapses the two parallel paths (`/swipe` + `/check-in`) into one. The chapter's existing `/swipe` route can stay for backwards-compat during rollout — point both at `performSwipeWrite` internally.

### 3. Replace `broadcastToUser` with `Broadcaster`

Today's `broadcastToUser(username, data)` is fine as-is, but uses ad-hoc payloads. Migrate gradually to the typed `SyncEvent` format so clients can use `LiveSync` with catch-up:

```js
// api/server.js — keep the existing function as a thin wrapper:
function broadcastToUser(username, data) {
  // Legacy callers pass arbitrary shapes (swipe events, partyEnd, etc.).
  // For the new swipe flow, callers use broadcaster.commit() directly.
  // For everything else, wrap as a generic 'event' payload:
  broadcaster.publish(username, { kind: 'commit', ...data, idempotencyKey: 'legacy', seqId: 0, duplicate: false, result: data, serverTs: Date.now() });
}
```

Then on the client, use `LiveSync` with the catch-up endpoint:

```js
import { LiveSync } from 'offline-sync-engine/client';

const live = new LiveSync({
  url: `${backendURL}/events?${new URLSearchParams({ user: username, token })}`,
  catchupUrl: (since) => `${backendURL}/events/since?user=${username}&since=${since}`,
  onCommit: (e) => handleSwipeEvent(e.result),
  initialSeqId: getStoredHighWaterMark(),
  onReconnect: (n, head) => {
    storeHighWaterMark(head);
    console.log(`[SSE] caught up ${n} events on reconnect`);
  },
});
live.start();
```

The `catchupUrl` requires a new endpoint:

```js
app.get('/events/since', checkJwt, async (req, res) => {
  const since = Number(req.query.since || 0);
  const events = await log.since(since, 1000);
  res.json({
    events: events.map((c) => ({ kind: 'commit', ...c })),
    headSeqId: await log.head(),
  });
});
```

This is the piece that fixes the "scanner B's WiFi blipped and missed 3 admits" bug.

### 4. Rollout plan

Don't migrate everything at once. Suggested order:

1. **Land the library + adapters** without changing existing behavior. Wire up `IdempotencyStore` and `MutationLog` server-side, but keep the existing `/swipe` and `/check-in` routes calling them in addition to the existing dedup logic. Compare metrics for a week.
2. **Switch new check-ins to the batch endpoint** behind a feature flag for one chapter (e.g. `@APD_ADMIN`). Watch for divergence.
3. **Migrate the IndexedDB store** on the client behind the same flag. Use a one-time migration to copy the existing `swipeQueue` rows into the new format with backfilled ULIDs (deterministic from name+timestamp so we don't double-write).
4. **Add `LiveSync` catch-up** as a pure enhancement — it doesn't change the write path, just makes scanner B not miss events. Turn on globally.
5. **Retire the old `_recentAdmissions` Map** once both paths route through `IdempotencyStore`.

### 5. What you keep vs replace

| Keep | Replace |
|---|---|
| `_deviceId` (use as `clientId`) | `_recentAdmissions` Map |
| `getCanonicalFratUsername`, `normalizeUsername` (domain logic) | `swipeQueue` IndexedDB store |
| Existing SSE endpoint shape (extend, don't break) | Custom timestamp-derived dedup keys |
| Stripe/billing flow | Naive retry-on-online listener |
| Firestore as the source of truth | The `/swipe` + `/check-in` two-route split |

## What this gives you

After full migration, the audit table from the README of `offline-sync-engine` describes FratDoor:

> Single scanner offline, queues 14 mutations → all 14 ship in one batch, server commits all 14 idempotently, no duplicates ever even if the response is lost.
>
> Scanner B's SSE drops for 8 seconds, scanner A admits 3 people → on B's reconnect, `LiveSync` fetches the 3 missed events from the catchup endpoint and replays them before resuming live updates.
>
> Server pod dies after writing a commit but before sending the response → client retries, server sees existing record, returns it with `duplicate: true`. No double-admit.

You'd be solving the actual hard version of the problem your portfolio post claims you solved.
