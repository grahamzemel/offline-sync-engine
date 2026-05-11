/**
 * Browser usage example: a check-in scanner that survives WiFi drops and
 * reconciles with peer scanners over SSE.
 */
import {
  OfflineQueue,
  LiveSync,
  IndexedDBQueueStorage,
} from 'offline-sync-engine/client';
import type { Mutation, CommitResult } from 'offline-sync-engine/client';

const topic = 'apartment-rooftop-party'; // per-event channel
const apiBase = '/api';

const storage = await IndexedDBQueueStorage.open('checkin-app');

const queue = new OfflineQueue({
  storage,
  // Batched sender hitting our /mutations endpoint
  sender: async (batch) => {
    const res = await fetch(`${apiBase}/mutations`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, mutations: batch }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.json();
    return body.results as CommitResult[];
  },
  onCommit: (m, r) => {
    // Update local state with server-assigned seqId so we know we're up to date.
    console.log(`✓ committed ${m.type} @ seq=${r.seqId}${r.duplicate ? ' (dup)' : ''}`);
  },
  onError: (m, err, attempts) => {
    console.warn(`✗ ${m.type} failed after ${attempts} attempts:`, err);
  },
});

// Live channel: see admits from other scanners and apply them locally.
const live = new LiveSync<{ name: string }>({
  url: `${apiBase}/events/${topic}`,
  catchupUrl: (sinceSeqId) => `${apiBase}/sync?since=${sinceSeqId}`,
  onPending: (e) => {
    // Another scanner is *about to* admit this person. Optimistically
    // show "pending" in the UI so we don't admit them again.
    console.log(`… ${e.clientId} announced ${e.idempotencyKey}`);
  },
  onCommit: (e) => {
    // A peer just committed — update our local model.
    console.log(`✓ peer commit seq=${e.seqId}`);
  },
  onReconnect: (n, head) => {
    console.log(`reconnect: caught up ${n} events, head=${head}`);
  },
});
live.start();

// Scanner UI calls this on every swipe.
export async function scan(name: string) {
  const key = await queue.enqueue('admit', { name });
  console.log(`queued ${key} locally; will sync when online`);
}
