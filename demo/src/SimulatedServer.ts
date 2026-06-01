/**
 * In-browser stand-in for what would be your Express/Fastify backend.
 *
 * Holds the truth: which guests have been admitted, in what order. Exposes
 * a single `submit(mutation)` that the scanner calls.
 *
 * Two failure modes are simulated:
 *
 *   1. WiFi OFF — the request never reaches the server (throw 'network').
 *
 *   2. "Ack lost" — the server FULLY processes the request, but the response
 *      fails in transit. The client sees a network error and retries. This
 *      is the realistic failure that idempotency keys solve. We fire it
 *      with ~25% probability per online request so the demo actually
 *      produces duplicates in naive mode within a few admits.
 *
 * In library mode, IdempotencyStore catches the retry and returns the
 * cached commit. In naive mode, the retry produces a fresh insert →
 * duplicate. That's the whole story the demo is trying to tell.
 */
import {
  IdempotencyStore,
  MemoryIdempotencyBackend,
  MutationLog,
} from 'offline-sync-engine/server';
import type { CommitResult, Mutation } from 'offline-sync-engine/server';

export interface AdmitPayload {
  guestId: string;
  guestName: string;
}

export interface AdmitRecord {
  seqId: number;
  guestId: string;
  guestName: string;
  serverTs: number;
}

const ACK_LOST_CHANCE = 0.25;

export class SimulatedServer {
  wifiOnline = true;
  useLibrary = false;

  private records: AdmitRecord[] = [];
  private duplicateAdmits = 0;
  private store = new IdempotencyStore(new MemoryIdempotencyBackend());
  private log = new MutationLog();

  private listeners = new Set<() => void>();
  onChange(fn: () => void): () => void {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }
  private emit() {
    this.listeners.forEach((fn) => fn());
  }

  reset() {
    this.records = [];
    this.duplicateAdmits = 0;
    this.store = new IdempotencyStore(new MemoryIdempotencyBackend());
    this.log = new MutationLog();
    this.emit();
  }

  totalRecorded(): number {
    return this.records.length;
  }
  duplicateCount(): number {
    return this.duplicateAdmits;
  }
  ledger(): AdmitRecord[] {
    return [...this.records];
  }

  async submit(mutation: Mutation<AdmitPayload>): Promise<CommitResult<AdmitRecord>> {
    if (!this.wifiOnline) {
      await new Promise((r) => setTimeout(r, 50));
      throw new Error('network');
    }
    await new Promise((r) => setTimeout(r, 80 + Math.random() * 60));

    if (this.useLibrary) {
      const result = await this.store.process<AdmitRecord>(
        mutation.idempotencyKey,
        async () => {
          const isGuestDup = this.records.some(
            (r) => r.guestId === mutation.payload.guestId
          );
          const seqId = await this.log.appendAndAssign({
            idempotencyKey: mutation.idempotencyKey,
            duplicate: false,
            result: null as unknown as AdmitRecord,
            serverTs: Date.now(),
          });
          const rec: AdmitRecord = {
            seqId,
            guestId: mutation.payload.guestId,
            guestName: mutation.payload.guestName,
            serverTs: Date.now(),
          };
          if (isGuestDup) this.duplicateAdmits++;
          this.records.push(rec);
          this.emit();
          return rec;
        },
        async () => 0
      );
      // Ack-lost: server is done, response fails in transit. Retry will
      // hit the IdempotencyStore and get the cached commit back. No
      // duplicate ever lands.
      if (Math.random() < ACK_LOST_CHANCE) {
        throw new Error('ack-lost');
      }
      return result;
    }

    // Naive mode: blindly insert, no server-side dedup.
    const isGuestDup = this.records.some(
      (r) => r.guestId === mutation.payload.guestId
    );
    const seqId = this.records.length + 1;
    const rec: AdmitRecord = {
      seqId,
      guestId: mutation.payload.guestId,
      guestName: mutation.payload.guestName,
      serverTs: Date.now(),
    };
    if (isGuestDup) this.duplicateAdmits++;
    this.records.push(rec);
    this.emit();
    // Same ack-lost simulation: the request landed, but the client never
    // hears back. Naive mode retries on next drain → inserts AGAIN.
    if (Math.random() < ACK_LOST_CHANCE) {
      throw new Error('ack-lost');
    }
    return {
      idempotencyKey: mutation.idempotencyKey,
      seqId,
      duplicate: false,
      result: rec,
      serverTs: Date.now(),
    };
  }
}
