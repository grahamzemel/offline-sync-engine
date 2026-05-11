import type { Mutation } from '../shared/types.js';

/**
 * Persisted queue entry. Wraps a Mutation with retry bookkeeping so the
 * client can apply backoff and surface persistent failures without losing
 * the mutation itself.
 */
export interface QueueEntry {
  mutation: Mutation;
  enqueuedAt: number;
  attempts: number;
  lastError?: string;
  lastAttemptAt?: number;
  /** Once committed, the server-assigned seqId is recorded before deletion. */
  committedSeqId?: number;
}

/**
 * Storage interface — pluggable so the queue works in browser (IndexedDB),
 * React Native (AsyncStorage), Electron, or even a Node service worker.
 * The library ships an IndexedDB and an in-memory adapter; bring your own
 * for everything else.
 */
export interface QueueStorage {
  put(entry: QueueEntry): Promise<void>;
  delete(idempotencyKey: string): Promise<void>;
  all(): Promise<QueueEntry[]>;
  get(idempotencyKey: string): Promise<QueueEntry | undefined>;
}

/**
 * In-memory adapter. Useful for tests, server-side rendering, and as a
 * fallback when persistent storage is unavailable. Loses data on reload —
 * never use in production for actual offline scenarios.
 */
export class MemoryQueueStorage implements QueueStorage {
  private map = new Map<string, QueueEntry>();

  async put(entry: QueueEntry): Promise<void> {
    this.map.set(entry.mutation.idempotencyKey, entry);
  }
  async delete(idempotencyKey: string): Promise<void> {
    this.map.delete(idempotencyKey);
  }
  async all(): Promise<QueueEntry[]> {
    return Array.from(this.map.values()).sort(
      (a, b) => a.enqueuedAt - b.enqueuedAt
    );
  }
  async get(idempotencyKey: string): Promise<QueueEntry | undefined> {
    return this.map.get(idempotencyKey);
  }
}

/**
 * IndexedDB adapter. The right choice for browsers — durable across reloads
 * and tabs, generous quota. Uses the optional `idb` peer dependency for a
 * nicer async wrapper; falls back to raw IndexedDB if `idb` isn't installed.
 *
 * @example
 * const storage = await IndexedDBQueueStorage.open('myapp');
 * const queue = new OfflineQueue({ storage, ... });
 */
export class IndexedDBQueueStorage implements QueueStorage {
  private constructor(
    private db: IDBDatabase,
    private storeName: string
  ) {}

  static async open(
    dbName = 'offline-sync-engine',
    storeName = 'queue'
  ): Promise<IndexedDBQueueStorage> {
    if (typeof indexedDB === 'undefined') {
      throw new Error(
        'IndexedDB is not available in this environment. Use MemoryQueueStorage or implement a custom QueueStorage.'
      );
    }
    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const req = indexedDB.open(dbName, 1);
      req.onerror = () => reject(req.error);
      req.onsuccess = () => resolve(req.result);
      req.onupgradeneeded = () => {
        const upgradeDb = req.result;
        if (!upgradeDb.objectStoreNames.contains(storeName)) {
          upgradeDb.createObjectStore(storeName, { keyPath: 'mutation.idempotencyKey' });
        }
      };
    });
    return new IndexedDBQueueStorage(db, storeName);
  }

  private tx(mode: IDBTransactionMode) {
    return this.db.transaction(this.storeName, mode).objectStore(this.storeName);
  }

  private wrap<T>(req: IDBRequest<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async put(entry: QueueEntry): Promise<void> {
    await this.wrap(this.tx('readwrite').put(entry));
  }
  async delete(idempotencyKey: string): Promise<void> {
    await this.wrap(this.tx('readwrite').delete(idempotencyKey));
  }
  async all(): Promise<QueueEntry[]> {
    const entries = (await this.wrap(this.tx('readonly').getAll())) as QueueEntry[];
    return entries.sort((a, b) => a.enqueuedAt - b.enqueuedAt);
  }
  async get(idempotencyKey: string): Promise<QueueEntry | undefined> {
    return (await this.wrap(this.tx('readonly').get(idempotencyKey))) as QueueEntry | undefined;
  }
}
