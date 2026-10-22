// NATS JetStream KV adapter for the VersionedSnapshots port (ai-fleet-snapshot-contracts-ts).
//
// One KV key per authority namespace is the linearization point: `<ns>.head` holds the writer fence and the
// accepted head, and every state change is a compare-and-swap on its KV revision. Everything else is
// content-addressed and immutable, written before the head CAS that makes it reachable:
//   <ns>.head                 { authority, fence, head, record }      CAS on revision (create for initialize)
//   <ns>.blob.<sha256>.<i>    chunk i of a blob                         images and history records
//   <ns>.op.<sha256(opId)>    record ref of the accepted operation      written after the head CAS
// Blobs are split into chunks below the server's max_payload (1 MiB on the cluster), so images and
// transactions of any size fit. A history record links to its predecessor; history walks that chain.
import { createHash } from "node:crypto";
import {
  apply, fingerprint, imageDigest, isExactDelta,
  type Classification, type CommitOutcome, type Head, type HistoryRecord, type Image, type Permit,
  type ServingHead, type Transaction, type VersionedSnapshots,
} from "@joshmouch/ai-fleet-snapshot-contracts-ts";
import { Kvm, type KV, type KvEntry } from "@nats-io/kv";
import type { NatsConnection } from "@nats-io/transport-node";

export type BlobRef = { readonly sha: string; readonly bytes: number; readonly chunks: number };

type HeadValue = { authority: string; fence: number; head: Head; record: BlobRef };
type RecordValue = { head: Head; transaction: Transaction | null; image: BlobRef; previous: BlobRef | null };
type Loaded = { revision: number; value: HeadValue };

export type OpenOptions = {
  /** KV bucket; created when absent. */
  bucket: string;
  /** Authority namespace inside the bucket: one VersionedSnapshots store per namespace. */
  namespace: string;
  /** Chunk size in bytes; default half the server's max_payload. */
  chunkBytes?: number;
  /** Bucket limits applied when the bucket is created. */
  maxBytes?: number;
  replicas?: number;
};

const NAME = /^[A-Za-z0-9_-]+$/;
const CAS_ATTEMPTS = 32;
const enc = new TextEncoder();
const dec = new TextDecoder();
const sha = (data: string | Uint8Array): string => createHash("sha256").update(data).digest("hex");

export class DamagedSnapshotError extends Error {}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const v of Object.values(value)) deepFreeze(v);
    Object.freeze(value);
  }
  return value;
}

export class NatsKvSnapshots implements VersionedSnapshots {
  readonly #kv: KV;
  readonly #ns: string;
  readonly #chunkBytes: number;
  readonly #indexed = new Set<string>();
  #cache: { sha: string; image: Image } | null = null;

  constructor(kv: KV, namespace: string, chunkBytes: number) {
    if (!NAME.test(namespace)) throw new Error(`namespace must match ${NAME}: ${namespace}`);
    if (!(Number.isInteger(chunkBytes) && chunkBytes > 0)) throw new Error("chunkBytes must be a positive integer");
    this.#kv = kv;
    this.#ns = namespace;
    this.#chunkBytes = chunkBytes;
  }

  /** Opens (creating when absent) the bucket on `nc` and binds one authority namespace in it. */
  static async open(nc: NatsConnection, options: OpenOptions): Promise<NatsKvSnapshots> {
    if (!NAME.test(options.bucket)) throw new Error(`bucket must match ${NAME}: ${options.bucket}`);
    const maxPayload = nc.info?.max_payload ?? 1024 * 1024;
    const chunkBytes = options.chunkBytes ?? Math.floor(maxPayload / 2);
    if (chunkBytes > maxPayload - 4096) throw new Error(`chunkBytes ${chunkBytes} exceeds max_payload ${maxPayload} less headers`);
    const kv = await new Kvm(nc).create(options.bucket, {
      history: 1, ...(options.maxBytes ? { max_bytes: options.maxBytes } : {}), ...(options.replicas ? { replicas: options.replicas } : {}),
    });
    return new NatsKvSnapshots(kv, options.namespace, chunkBytes);
  }

  async classify(): Promise<Classification> {
    const loaded = await this.#head();
    if (!loaded) return "uninitialized";
    try {
      const record = await this.#record(loaded.value.record);
      const image = await this.#image(record.image);
      return imageDigest(image) === loaded.value.head.digest && record.head.digest === loaded.value.head.digest ? "live" : "damaged";
    } catch (error) {
      if (error instanceof DamagedSnapshotError) return "damaged";
      throw error;
    }
  }

  async initialize(image: Image): Promise<Head> {
    if (await this.#head()) throw new Error("authority already initialized");
    const { value } = await this.#install(image, null, "", "", null, 0);
    try {
      await this.#kv.create(this.#key("head"), enc.encode(JSON.stringify(value)));
    } catch (error) {
      if (await this.#head()) throw new Error("authority already initialized");
      throw error;
    }
    return deepFreeze({ ...value.head });
  }

  async activate(fence: number): Promise<ServingHead> {
    for (let i = 0; i < CAS_ATTEMPTS; i++) {
      const loaded = await this.#head();
      if (!loaded) throw new Error("uninitialized");
      if (!(fence > loaded.value.fence)) throw new Error("activation fence must be strictly newer");
      const value: HeadValue = { ...loaded.value, fence };
      if (await this.#swap(loaded, value)) return serving(value);
    }
    throw new Error("activate: head kept changing");
  }

  async read(): Promise<{ image: Image; head: Head }> {
    const loaded = await this.#head();
    if (!loaded) throw new Error("uninitialized");
    const record = await this.#record(loaded.value.record);
    return { image: await this.#image(record.image), head: deepFreeze({ ...loaded.value.head }) };
  }

  async commit(permit: Permit, tx: Transaction): Promise<CommitOutcome> {
    for (let i = 0; i < CAS_ATTEMPTS; i++) {
      const loaded = await this.#head();
      if (!loaded) return { kind: "refused", reason: "uninitialized" };
      const current = loaded.value;
      if (permit.authority !== current.authority || permit.fence !== current.fence) return { kind: "refused", reason: "stale-fence" };
      const record = await this.#record(current.record);
      await this.#index(current.record, record);
      const fp = fingerprint(tx);
      const prior = record.transaction?.operationId === tx.operationId ? record : await this.#operation(tx.operationId);
      if (prior) {
        return prior.head.fingerprint === fp
          ? { kind: "replayed", head: deepFreeze({ ...prior.head, permit: { authority: current.authority, fence: current.fence } }) }
          : { kind: "refused", reason: "conflict" };
      }
      if (tx.expectedCoordinate !== current.head.coordinate || tx.expectedReceipt !== current.head.receipt) {
        return { kind: "refused", reason: "expected-head" };
      }
      const image = await this.#image(record.image);
      if (!isExactDelta(image, tx)) return { kind: "refused", reason: "invalid-transaction" };
      const next = apply(image, tx);
      if (imageDigest(next) !== tx.resultDigest) return { kind: "refused", reason: "invalid-transaction" };
      const installed = await this.#install(next, tx, fp, current.head.receipt, current.record, current.fence);
      if (await this.#swap(loaded, installed.value)) {
        await this.#index(installed.value.record, installed.record);
        return { kind: "accepted", head: serving(installed.value) };
      }
    }
    throw new Error("commit: head kept changing");
  }

  async history(): Promise<readonly HistoryRecord[]> {
    const loaded = await this.#head();
    if (!loaded) return [];
    const out: HistoryRecord[] = [];
    for (let ref: BlobRef | null = loaded.value.record; ref; ) {
      const record = await this.#record(ref);
      out.push(deepFreeze({ head: { ...record.head }, transaction: record.transaction }));
      ref = record.previous;
    }
    return out.reverse();
  }

  async exportImage(): Promise<{ image: Image; digest: string }> {
    const { image, head } = await this.read();
    return { image, digest: head.digest };
  }

  async importImage(image: Image, digest: string): Promise<Head> {
    if (imageDigest(image) !== digest) throw new Error("import digest mismatch");
    return this.initialize(image);
  }

  #key(...parts: string[]): string { return [this.#ns, ...parts].join("."); }

  async #head(): Promise<Loaded | null> {
    const entry: KvEntry | null = await this.#kv.get(this.#key("head"));
    if (!entry || entry.operation !== "PUT") return null;
    return { revision: entry.revision, value: entry.json<HeadValue>() };
  }

  /** CAS on the head revision; false when another writer moved the head first. */
  async #swap(loaded: Loaded, value: HeadValue): Promise<boolean> {
    try {
      await this.#kv.update(this.#key("head"), enc.encode(JSON.stringify(value)), loaded.revision);
      return true;
    } catch (error) {
      const now = await this.#head();
      if (now && now.revision !== loaded.revision) return false;
      throw error;
    }
  }

  /** Writes the image and its history record (both immutable) and returns the head value that would make them current. */
  async #install(image: Image, tx: Transaction | null, fp: string, previousReceipt: string, previous: BlobRef | null, fence: number) {
    const digest = imageDigest(image);
    const head: Head = {
      coordinate: image.coordinate,
      receipt: `nats-kv:${sha(JSON.stringify([previousReceipt, image.coordinate, digest, fp]))}`,
      digest, fingerprint: fp,
    };
    const imageRef = await this.#putBlob(enc.encode(JSON.stringify({ ...image, entries: [...image.entries].sort(byAddress) })));
    const record: RecordValue = { head, transaction: tx, image: imageRef, previous };
    const recordRef = await this.#putBlob(enc.encode(JSON.stringify(record)));
    return { value: { authority: image.authority, fence, head, record: recordRef } as HeadValue, record };
  }

  /** Operation index entries are written after acceptance; the current head's own entry is repaired on the next commit. */
  async #index(ref: BlobRef, record: RecordValue): Promise<void> {
    if (!record.transaction || this.#indexed.has(ref.sha)) return;
    const key = this.#key("op", sha(record.transaction.operationId));
    const existing = await this.#kv.get(key);
    if (!existing || existing.operation !== "PUT") await this.#kv.put(key, enc.encode(JSON.stringify(ref)));
    this.#indexed.add(ref.sha);
  }

  async #operation(operationId: string): Promise<RecordValue | null> {
    const entry = await this.#kv.get(this.#key("op", sha(operationId)));
    if (!entry || entry.operation !== "PUT") return null;
    return this.#record(entry.json<BlobRef>());
  }

  async #record(ref: BlobRef): Promise<RecordValue> {
    return JSON.parse(dec.decode(await this.#getBlob(ref))) as RecordValue;
  }

  async #image(ref: BlobRef): Promise<Image> {
    if (this.#cache?.sha !== ref.sha) this.#cache = { sha: ref.sha, image: deepFreeze(JSON.parse(dec.decode(await this.#getBlob(ref))) as Image) };
    return this.#cache.image;
  }

  async #putBlob(bytes: Uint8Array): Promise<BlobRef> {
    const ref: BlobRef = { sha: sha(bytes), bytes: bytes.length, chunks: Math.max(1, Math.ceil(bytes.length / this.#chunkBytes)) };
    await Promise.all(Array.from({ length: ref.chunks }, (_, i) =>
      this.#kv.put(this.#key("blob", ref.sha, String(i)), bytes.subarray(i * this.#chunkBytes, (i + 1) * this.#chunkBytes))));
    return ref;
  }

  async #getBlob(ref: BlobRef): Promise<Uint8Array> {
    const parts = await Promise.all(Array.from({ length: ref.chunks }, (_, i) => this.#kv.get(this.#key("blob", ref.sha, String(i)))));
    const out = new Uint8Array(ref.bytes);
    let offset = 0;
    for (const [i, part] of parts.entries()) {
      if (!part || part.operation !== "PUT") throw new DamagedSnapshotError(`blob ${ref.sha} chunk ${i} missing`);
      if (offset + part.value.length > ref.bytes) throw new DamagedSnapshotError(`blob ${ref.sha} longer than ${ref.bytes}`);
      out.set(part.value, offset);
      offset += part.value.length;
    }
    if (offset !== ref.bytes || sha(out) !== ref.sha) throw new DamagedSnapshotError(`blob ${ref.sha} failed its digest`);
    return out;
  }
}

function byAddress(x: { address: string }, y: { address: string }): number {
  return x.address < y.address ? -1 : x.address > y.address ? 1 : 0;
}

function serving(value: HeadValue): ServingHead {
  return deepFreeze({ ...value.head, permit: { authority: value.authority, fence: value.fence } });
}
