# ai-fleet-snapshot-nats-ts

NATS JetStream KV adapter for the `VersionedSnapshots` port
([ai-fleet-snapshot-contracts-ts](https://github.com/joshmouch/ai-fleet-snapshot-contracts-ts)): Conflux Durable
Authority Storage's `snapshot-store:nats-kv` binding (ai-fleet
`docs/plans/active/2026-10-16-service-adapters-and-state-ownership` decision 7).

```ts
import { connect } from "@nats-io/transport-node";
import { NatsKvSnapshots } from "@joshmouch/ai-fleet-snapshot-nats-ts";

const nc = await connect({ servers: "nats://nats.nats.svc.cluster.local:4222" });
const store = await NatsKvSnapshots.open(nc, { bucket: "ai_fleet_snapshots", namespace: "fleet-organizer" });
```

## Layout in the bucket

One namespace is one authority. Its `head` key is the only mutable key and the single linearization point;
everything else is immutable and content-addressed, written before the head compare-and-swap that makes it
reachable.

| Key | Value | Written by |
| --- | --- | --- |
| `<ns>.head` | `{ authority, fence, head, record }` | `create` (initialize/import), `update` on the read revision (activate, commit) |
| `<ns>.blob.<sha256>.<i>` | chunk `i` of an image or history-record blob | `put`, before the head CAS |
| `<ns>.op.<sha256(operationId)>` | ref of the record that accepted the operation | `put`, after the head CAS; the current head's entry is repaired by the next commit |

| Contract requirement | How |
| --- | --- |
| Initialize an absent authority only; reopen | `kv.create` on `head` fails when it exists; any instance on the same namespace reopens |
| Strictly newer writer fence | `activate` CASes `fence` into `head`; `commit` refuses a permit whose fence differs |
| Compare-and-swap on expected head | expected coordinate + receipt checked against `head`, then `kv.update(head, revision)`; a lost race re-evaluates |
| Idempotent replay / conflict | operation index plus the current head's own transaction, compared by `fingerprint` |
| Immutable history | each record links to its predecessor; `history()` walks the chain |
| Full image export/import | `exportImage` / `importImage` bound to `ImageDigest` |
| Messages above max_payload | blobs split into chunks of half the server's `max_payload` (512 KiB on the cluster's 1 MiB limit), digest-checked on read; a missing or altered chunk classifies the store `damaged` |

## Test

`npm test` runs the shared `runConformance` suite (default chunking and 64-byte chunks), a 3.5 MiB image and
2 MiB transaction, concurrent writers and a damaged store against a live server: `SNAPSHOT_NATS_URL`, default
`nats://native-archive-broker.tail8400d9.ts.net:4222` (the cluster's JetStream on the Tailnet). It uses a
throwaway bucket `snapshot_conformance_<random>` and destroys it.

The contracts package ships TypeScript sources, which Node's native type stripping refuses under
`node_modules`, so tests run through `tsx` (`node --import tsx --test test/`).
