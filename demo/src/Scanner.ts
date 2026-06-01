/**
 * Single door scanner. Two modes:
 *
 *   Library ON  → backed by OfflineQueue + IndexedDB. Admits get a ULID,
 *                 persist locally, retry with backoff. Caller's "await admit()"
 *                 returns the moment the mutation is durably queued — never
 *                 waits for the network.
 *
 *   Library OFF → "naive" baseline a typical app might ship: try the network,
 *                 if it fails push the request onto an in-memory backlog and
 *                 retry on a timer. Failure modes:
 *                   (a) loses admits if you reload the tab while offline
 *                   (b) double-submits when the server got the request but
 *                       the response failed in transit — the retry has no
 *                       idempotency context so the server inserts again.
 */
import { OfflineQueue, MemoryQueueStorage, ulid } from 'offline-sync-engine/client';
import type { Mutation, CommitResult } from 'offline-sync-engine/client';
import type { SimulatedServer, AdmitPayload, AdmitRecord } from './SimulatedServer.js';

export type EventKind = 'queued' | 'commit' | 'retrying' | 'error';
export interface ScannerEvent {
  kind: EventKind;
  guestName: string;
  detail?: string;
  at: number;
}

export interface PendingItem {
  idempotencyKey: string;
  guestName: string;
  clientTs: number;
}

export class Scanner {
  private server: SimulatedServer;
  private getUseLibrary: () => boolean;
  private getOnline: () => boolean;

  // Library mode
  private queue: OfflineQueue | null = null;

  // Naive mode
  private naiveBacklog: Mutation<AdmitPayload>[] = [];
  private naiveDraining = false;

  private listeners = new Set<(e: ScannerEvent) => void>();
  private events: ScannerEvent[] = [];

  constructor(
    server: SimulatedServer,
    getUseLibrary: () => boolean,
    getOnline: () => boolean
  ) {
    this.server = server;
    this.getUseLibrary = getUseLibrary;
    this.getOnline = getOnline;
    this.initQueue();
  }

  private initQueue() {
    this.queue = new OfflineQueue({
      storage: new MemoryQueueStorage(),
      sender: async (batch: Mutation[]) => this.sendBatch(batch),
      initialBackoffMs: 300,
      maxBackoffMs: 2000,
      onCommit: (m, r) => {
        const result = r as CommitResult<AdmitRecord>;
        this.pushEvent({
          kind: 'commit',
          guestName: (m as Mutation<AdmitPayload>).payload.guestName,
          detail: result.duplicate
            ? `server already had it (seq ${result.seqId}) — idempotency saved us`
            : `recorded as seq ${result.seqId}`,
          at: Date.now(),
        });
      },
    });
  }

  reset() {
    this.naiveBacklog = [];
    this.naiveDraining = false;
    this.events = [];
    this.initQueue();
    this.listeners.forEach((fn) =>
      fn({ kind: 'commit', guestName: '', at: Date.now() })
    );
  }

  /** Called when the WiFi toggle flips ON — trigger queue drain. */
  onNetworkResume() {
    if (this.getUseLibrary()) {
      void this.queue?.flush();
    } else {
      void this.naiveDrain();
    }
  }

  async pendingCount(): Promise<number> {
    if (this.getUseLibrary()) return this.queue ? await this.queue.size() : 0;
    return this.naiveBacklog.length;
  }

  /** What's sitting in the queue, with names for the UI. */
  async getPending(): Promise<PendingItem[]> {
    if (this.getUseLibrary()) {
      if (!this.queue) return [];
      const items = await this.queue.pending();
      return items.map((m) => {
        const mp = m as Mutation<AdmitPayload>;
        return {
          idempotencyKey: mp.idempotencyKey,
          guestName: mp.payload.guestName,
          clientTs: mp.clientTs,
        };
      });
    }
    return this.naiveBacklog.map((m) => ({
      idempotencyKey: m.idempotencyKey,
      guestName: m.payload.guestName,
      clientTs: m.clientTs,
    }));
  }

  onEvent(fn: (e: ScannerEvent) => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
  recentEvents(limit = 30): ScannerEvent[] {
    return this.events.slice(-limit).reverse();
  }

  private pushEvent(e: ScannerEvent) {
    this.events.push(e);
    if (this.events.length > 200) this.events.shift();
    this.listeners.forEach((fn) => fn(e));
  }

  async admit(payload: AdmitPayload) {
    if (this.getUseLibrary()) {
      // Returns the moment the mutation is in IndexedDB. No await on network.
      await this.queue!.enqueue('admit', payload);
      this.pushEvent({
        kind: 'queued',
        guestName: payload.guestName,
        detail: this.getOnline() ? 'syncing…' : 'offline — saved locally',
        at: Date.now(),
      });
      return;
    }
    // Naive: try direct, fall back to in-memory backlog.
    const mutation: Mutation<AdmitPayload> = {
      idempotencyKey: ulid(),
      type: 'admit',
      payload,
      clientTs: Date.now(),
    };
    this.pushEvent({
      kind: 'queued',
      guestName: payload.guestName,
      detail: this.getOnline() ? 'sending…' : 'offline — in-memory (lost on reload)',
      at: Date.now(),
    });
    this.naiveBacklog.push(mutation);
    void this.naiveDrain();
  }

  /**
   * Naive drain loop. Just retry on any error — no idempotency.
   * The duplicates emerge naturally: the server simulates "ack lost"
   * by inserting the record and then throwing, and the retry inserts
   * a fresh row because nothing on the server knows it was already
   * processed.
   */
  private async naiveDrain() {
    if (this.naiveDraining) return;
    this.naiveDraining = true;
    try {
      while (this.naiveBacklog.length > 0) {
        const m = this.naiveBacklog[0];
        try {
          await this.server.submit(m);
          this.naiveBacklog.shift();
          this.pushEvent({
            kind: 'commit',
            guestName: m.payload.guestName,
            detail: 'sent (no dedup — server takes what it gets)',
            at: Date.now(),
          });
        } catch (e) {
          const msg = (e as Error).message;
          this.pushEvent({
            kind: 'retrying',
            guestName: m.payload.guestName,
            detail: msg === 'ack-lost'
              ? 'ack lost — retrying (may produce duplicate)'
              : 'offline — waiting for WiFi',
            at: Date.now(),
          });
          await new Promise((r) => setTimeout(r, 350));
          if (!this.getOnline()) return; // stop trying until WiFi comes back
        }
      }
    } finally {
      this.naiveDraining = false;
    }
  }

  private async sendBatch(batch: Mutation[]): Promise<CommitResult[]> {
    // Per-item submit. Items that hit ack-lost return undefined; the
    // queue keeps them in storage with attempts++ and schedules a retry.
    // Items that succeed commit cleanly and get removed. Either way, the
    // IdempotencyStore on the server guarantees zero duplicates.
    const results: (CommitResult | undefined)[] = [];
    for (const m of batch) {
      try {
        results.push(await this.server.submit(m as Mutation<AdmitPayload>));
      } catch {
        results.push(undefined);
      }
    }
    return results as CommitResult[];
  }
}
