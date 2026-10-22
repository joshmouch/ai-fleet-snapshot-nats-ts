// Runs the shared snapshot-store conformance suite against a live NATS JetStream server
// (SNAPSHOT_NATS_URL, default the cluster's Tailnet broker) in a throwaway bucket that is destroyed afterwards.
import { after, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { connect } from "@nats-io/transport-node";
import { Kvm } from "@nats-io/kv";
import { imageDigest, transactionBetween, type Image } from "@joshmouch/ai-fleet-snapshot-contracts-ts";
import { runConformance, fixtureBase } from "@joshmouch/ai-fleet-snapshot-contracts-ts/conformance";
import { NatsKvSnapshots } from "../src/index.ts";

const url = process.env.SNAPSHOT_NATS_URL ?? "nats://native-archive-broker.tail8400d9.ts.net:4222";
const nc = await connect({ servers: url, name: "ai-fleet-snapshot-nats-ts conformance" });
const bucket = `snapshot_conformance_${randomBytes(6).toString("hex")}`;
const kv = await new Kvm(nc).create(bucket, { history: 1, max_bytes: 256 * 1024 * 1024 });
const maxPayload = nc.info!.max_payload;
let n = 0;
const fresh = (chunkBytes = Math.floor(maxPayload / 2)) => new NatsKvSnapshots(kv, `t${++n}`, chunkBytes);

after(async () => {
  await kv.destroy();
  await nc.close();
});

runConformance("NatsKvSnapshots", async () => fresh());
runConformance("NatsKvSnapshots (64-byte chunks)", async () => fresh(64));

async function chunkKeys(namespace: string): Promise<string[]> {
  const keys: string[] = [];
  for await (const k of await kv.keys(`${namespace}.blob.>`)) keys.push(k);
  return keys;
}

test("images and transactions larger than max_payload are chunked", async () => {
  const big = (seed: string, mib: number) => seed.repeat(Math.ceil((mib * 1024 * 1024) / seed.length));
  const image: Image = { ...fixtureBase, entries: [
    { address: "nodes/big", content: big("x", 1.5) }, { address: "nodes/n2", content: big("y", 1.5) },
  ] };
  const s = fresh();
  const ns = `t${n}`;
  const h = await s.initialize(image);
  assert.ok((await chunkKeys(ns)).length >= 6, "a 3 MiB image is stored as several sub-payload chunks");
  assert.equal((await s.read()).head.digest, h.digest);
  assert.equal(imageDigest((await s.read()).image), imageDigest(image));

  const live = await s.activate(1);
  const next: Image = { ...image, coordinate: 2, entries: [image.entries[0], { address: "nodes/n2", content: big("z", 2) }] };
  const out = await s.commit(live.permit, transactionBetween(image, live.receipt, next, "big-op", "m"));
  assert.equal(out.kind, "accepted");
  const reopened = new NatsKvSnapshots(kv, ns, Math.floor(maxPayload / 2));
  assert.equal(await reopened.classify(), "live");
  assert.equal((await reopened.history()).at(-1)!.transaction!.writes[0].content.length, next.entries[1].content.length);

  const { image: exported, digest } = await reopened.exportImage();
  assert.equal((await fresh().importImage(exported, digest)).digest, digest);
});

test("concurrent writers: exactly one compare-and-swap wins", async () => {
  const a = fresh();
  const b = new NatsKvSnapshots(kv, `t${n}`, Math.floor(maxPayload / 2));
  await a.initialize(fixtureBase);
  const live = await a.activate(1);
  const tx = (op: string, content: string) =>
    transactionBetween(fixtureBase, live.receipt, { ...fixtureBase, coordinate: 2, entries: [{ address: "nodes/n1", content }] }, op, "m");
  const outcomes = await Promise.all([a.commit(live.permit, tx("op-a", "A")), b.commit(live.permit, tx("op-b", "B"))]);
  assert.deepEqual(outcomes.map((o) => o.kind).sort(), ["accepted", "refused"]);
  assert.equal((await b.history()).length, 2);
  const fences = await Promise.allSettled([a.activate(5), b.activate(5)]);
  assert.deepEqual(fences.map((f) => f.status).sort(), ["fulfilled", "rejected"]);
});

test("a missing chunk classifies the store as damaged", async () => {
  const s = fresh(64);
  await s.initialize(fixtureBase);
  const [first] = await chunkKeys(`t${n}`);
  await kv.purge(first);
  assert.equal(await new NatsKvSnapshots(kv, `t${n}`, 64).classify(), "damaged");
});
