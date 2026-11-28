# Predastore Design

Predastore is a distributed, S3-compatible object store. It combines Reed–Solomon erasure coding, a Raft-replicated metadata plane, QUIC transport between hosts, and append-only segment storage sealed with AES-256-GCM.

This document is the implementation guide: it describes what the code does today, not what it might do. Where the implementation falls short of the intent, §15 says so explicitly. For the operator-facing view — installing, running, configuring — start with the [README](../README.md).

## Contents

1. [Reading order](#1-reading-order)
2. [Cluster model](#2-cluster-model)
3. [Process lifecycle](#3-process-lifecycle)
4. [Transports and rpc](#4-transports-and-rpc)
5. [The gate](#5-the-gate)
6. [The meta plane](#6-the-meta-plane)
7. [The blob plane](#7-the-blob-plane)
8. [Object lifecycle](#8-object-lifecycle)
9. [Erasure coding and placement](#9-erasure-coding-and-placement)
10. [Encryption at rest](#10-encryption-at-rest)
11. [Security](#11-security)
12. [Configuration](#12-configuration)
13. [Failure handling](#13-failure-handling)
14. [Developer reference](#14-developer-reference)
15. [Known gaps](#15-known-gaps)

---

# 1. Reading order

The tree has four layers, and each one only knows about the layer below it:

| Layer | Package | What it owns |
|---|---|---|
| Process | `predastore` (root), `cmd/s3d` | Flags, the config file, and building one host's nodes |
| Roles | `internal/gate`, `internal/meta`, `internal/blob` | The three things a node can be |
| Plumbing | `internal/rpc`, `internal/transport` | Node-addressed streams over a pipe or QUIC |
| Storage | `internal/blob/engine` | Segments, fragments, extents, encryption at rest |

`internal/config` sits underneath all of it. It parses the TOML file and validates it, and answers nothing else: placement, addressing and the conversion into each subsystem's own settings all live above it.

Design goals, in the order they win arguments:

- Strong consistency for metadata. Every object's placement is a Raft commit.
- Durability through erasure coding, not replication. `K` of `K+M` shards rebuild an object.
- No plaintext on disk, ever. There is no unencrypted code path.
- One binary, one config file, one flag to say which machine you are.
- Scale down as well as up: RS(1,0) on one host is the same code as RS(3,2) on five.

---

# 2. Cluster model

A cluster is two levels. A **host** is one machine running one `s3d` process, owning an address, a data directory and a TLS identity. A **node** is a role pinned to a host, running inside that process as a goroutine on its own port.

<p align="center">
  <img src="assets/cluster-topology.svg" alt="Predastore cluster topology: S3 clients reach a gate node over HTTPS with Signature V4; each host is one s3d process running gate, meta and blob nodes that talk over an in-process pipe, while nodes on different hosts talk over QUIC" width="900">
</p>

## Roles

| Role | Purpose | Listens | Dials |
|---|---|---|---|
| `gate` | Serves the S3 API: SigV4, IAM, erasure coding, placement | HTTPS on its `port` | meta and blob nodes |
| `meta` | Raft replica over global state | rpc on its `port` | its meta peers |
| `blob` | Stores erasure-coded shards | rpc on its `port` | nothing |

A gate is a node with a role, not a per-host special case. A host may declare at most one — two would be two S3 endpoints answering for one machine — and a host declaring none simply serves no S3 API. Because nothing dials a gate, its rpc transports bind ephemerally and it registers no listener; its `port` is the S3 port and nothing else.

Node ids are unique across the whole file, because the rpc resolver keys one flat table by id. Ports are unique within a host, because each node binds its own socket rather than sharing the host's.

## Deployment shapes

There are no modes. Which transport a pair of nodes uses follows from where the config pins them: same host is an in-process pipe, different hosts is QUIC. A cluster whose nodes all sit under one `[[host]]` therefore runs entirely in one process and opens no rpc socket at all — that is a property of the config, not a launch flag. The three profiles in `config/` (`1host`, `3host`, `5host`) differ only in how the nodes are spread.

---

# 3. Process lifecycle

`cmd/s3d` is a thin entrypoint: parse flags, resolve the host-local settings, load the master key, call `predastore.Run`.

```bash
./bin/s3d -config cluster.toml -host 1 \
  -data-dir /var/lib/predastore \
  -tls-cert /etc/predastore/server.pem -tls-key /etc/predastore/server.key \
  -encryption-key /etc/predastore/master.key
```

| Flag | Overrides | Notes |
|---|---|---|
| `-config` | — | Required |
| `-host` | — | Required; selects the `[[host]]` this process is |
| `-bind-addr` | `bind_addr` | Cluster-plane listen address, no port; defaults to `addr` |
| `-gate-bind-addr` | the gate's `bind_addr` | S3 listen address, no port; defaults to the host's |
| `-data-dir` | `data_dir` | On-disk root for this host's nodes |
| `-encryption-key` | `encryption_key` | 32 raw bytes, mode `0600` |
| `-tls-cert`, `-tls-key` | `tls_cert`, `tls_key` | This host's TLS identity |
| `-log-level` | — | `debug`, `info`, `warn`, `error` |

There are no environment variables. The host-local fields resolve flag first, then file, then default, and are written back into the parsed config so the rest of the tree reads one settled value. The file's own validation then runs a second time over the merged tree, because `-data-dir` can supply a root the file never had and a node's derived directory only collides with an explicit one once that root is known.

`predastore.Run` builds every node of the host before starting any of them — a node that dialed a colocated peer first would otherwise find no listener registered for it — then runs them under one errgroup sharing one context. A single signal stops the lot; there is nothing else to stop it with. `buildNode` creates each node's transports, its resolver, its listeners and its service; whatever it acquired before a failure is released by the cleanup it returns.

## The leader barrier

A gate that started serving before local consensus settled would fail writes that would have succeeded a moment later. So a host running a meta replica holds its gate off until a leader is observed, or until `LeaderTimeout` (30s) expires without one — a slow election still releases the gate rather than never serving. A host with no replica of its own has no local consensus to wait on and starts open.

---

# 4. Transports and rpc

## Two transports, chosen by the config

`internal/transport` provides `Transport`, `Listener`, `Conn` and `Stream`, implemented twice:

- **pipe** (`NetworkPipe`) — in-process, for nodes sharing a host. No sockets, no handshake, no TLS.
- **quic** (`NetworkQUIC`) — one bound UDP socket per node, TLS 1.3, ALPN pinned to `mulga-repl-v1`.

Every node owns its own UDP socket, bound from its host's `bind_addr` and its own port. Colocated nodes do not share a socket, so a QUIC address is a plain `host:port` and the listener a node accepts on is its own. A node only builds a pipe transport when its host runs more than one node, and only builds a QUIC transport when the cluster has nodes on other hosts.

## Addressing

Callers name peers by node id and never handle an address. `rpc.Resolver` is the flat route table — `NodeID → Route{Transport, Addr}` — built once from the configuration. It fails at construction if a route it must serve has no matching transport, rather than at the dial it would have served. Gates are left out of the table entirely, since nothing dials one. `Resolver.NodeAt` reverses the table, naming the node behind an inbound address.

## Connection pooling

`rpc.ConnPool` keys connections by node id, so a connection dialed to a peer and one accepted from it are the same entry on either transport, and a replica pair reuses one connection in both directions. An accepted connection is offered to the pool by `Donate`, which reverses the route table to name the peer behind it; a peer dialing from an ephemeral socket resolves to no node and is simply not pooled. Reuse in both directions only holds if both ends answer streams on one connection, so `rpc.Server` serves what the pool dials as well as what a listener accepts: the pool hands each connection it opens to the server through `OnDial`, and a node with no server of its own — a gate — simply registers nothing and sends over what it dialed without being sent anything back.

The tiebreak that keeps one connection per pair is the same whether the slot is empty or not: each end keeps the connection the lower of the two node ids dialed, which both compute from the same two ids. An empty slot therefore refuses a donation the node would rather have dialed itself, and the peer keeps that connection for its own outbound use until this node's dial displaces it. A node's own dial is always kept, since refusing it would leave the end that prefers to accept with no way to open a connection at all.

The pool runs no idle sweeper. A connection leaves through `Evict`, called by whichever side observes it die, or through `Close` when the node shuts down. It also leaves once `maxStreamStalls` (3) consecutive streams opened on it get no response byte at all, which is the only evidence available that a connection alive at the transport is answering nothing at the rpc layer; any response clears the run. Concurrent dials to one peer collapse onto a single connection via singleflight.

| Layer | Lifetime | Cost |
|---|---|---|
| Connection | Long-lived, pooled | TLS handshake, once per peer pair |
| Stream | Per request | A stream id |

## Stream framing

Every request is one stream. The stream opens with an 8-byte prefix — big-endian `uint32` opcode, then big-endian `uint32` header length — followed by the encoded header, followed by whatever body the operation carries. Headers are JSON and capped at 1 MiB. Opcodes are allocated per service in non-overlapping ranges, so a stream's opcode identifies the service that answers it:

| Opcode | Service | Operation |
|---|---|---|
| `0x0001` | meta | Raft dial; the stream carries the raft wire protocol for its lifetime |
| `0x1001`–`0x1004` | meta | get, put, delete, scan |
| `0x2001`–`0x2003` | blob | get, put, delete |

Both halves of a stream must be closed. Both the dial and the listen side cap a connection at 1000 concurrent incoming streams; unclosed streams block `OpenStream` once that cap is reached. The caps match deliberately — a reused connection is dialed by one side and accepted by the other, so a lower dial-side cap would silently throttle it.

QUIC tuning lives in `internal/transport/quic.go`: 15s keepalive, 60s max idle, 5s handshake idle, 2 MiB initial and 8 MiB max stream receive windows, 16 MiB initial and 128 MiB max connection receive windows.

---

# 5. The gate

`internal/gate` is the S3 frontend: an HTTPS listener, a middleware chain, and a chi route table. The operations themselves are in `internal/gate/handlers`, credential resolution in `internal/gate/auth`, placement in `internal/gate/placement`, and the S3 vocabulary — error taxonomy, name validation, stored record shapes — in `internal/gate/model`.

The gate is the S3 implementation, not a front end onto one. It erasure codes, places and records its own operations; meta and blob are storage it drives, not services it forwards to.

## Request path

1. **Global middleware**, registered before chi has matched, so none of it may read a bucket or key: OTel HTTP instrumentation, panic recovery, and `StripSlashes` (S3 accepts `PUT /bucket/` as `PUT /bucket`; only chi's routing context is rewritten, so SigV4 still sees the URI the client signed).
2. **Route match**, into one of three groups — no resource, a bucket, or an object.
3. **Resource resolution**, as inline middleware inside each group: the bucket or object is validated and put on the context. Everything downstream authorizes and acts on that one resolved resource, so the subject checked and the subject acted on cannot diverge. A malformed name answers `InvalidBucketName` or `InvalidKey`, never `AccessDenied`, since this runs before authentication.
4. **Auth**: public-bucket short-circuit for GET and HEAD, then SigV4 verification, then IAM policy evaluation, then the cross-account bucket-ownership check.
5. **Throttling**, keyed by account and action — after auth, so the limit counts against the authenticated account.

## Serving

TLS 1.3 minimum, HTTP/2 advertised over ALPN with HTTP/1.1 as fallback. Read and write timeouts 60s, idle 120s, header read 10s, max header 1 MiB. `Run` serves until the context is cancelled, then drains in-flight requests within a 10s grace period. A gate that cannot bind takes the process down rather than leave the cluster running headless.

## VPC endpoint listener

A gate with `endpoint_port` set serves the same routes a second time, to guests that reach S3 through a VPC endpoint.
A node's endpoint proxy captures the guest's connection to the S3 service address and relays it here; the guest's own TLS still terminates at the gate, against the same certificate as the S3 port.

Each connection carries two layers before the guest's bytes:

1. **Node session.** TLS 1.3, with a client certificate that chains to the formation CA in `tls_ca` and carries the client-authentication usage. Node certificates have it; guest service certificates signed by the same CA have server authentication only, so they are refused.
2. **PROXY v2 header.** One header per connection, read in full before the guest's first byte, then the stream is handed to the HTTP server.

A request on this listener is addressed as the guest: `RemoteAddr` is the header's source, and its context carries the endpoint.
Authorisation then supplies `aws:SourceVpce`, `aws:SourceVpc` and `aws:VpcSourceIp` and omits `aws:SourceIp`, as AWS does for a request through a gateway endpoint.
A `NotIpAddress` deny on `aws:SourceIp` therefore holds, and an allow on it does not match.

The handshake and header read share a 10s deadline, and each connection is admitted concurrently so a stalled peer blocks no other.
Shutdown closes every connection still in its handshake rather than waiting out that deadline.

### Proxy-to-gate contract

The header is a cross-component contract between spinifex's endpoint proxy (producer) and this listener (consumer).
The codec is `endpointproxy`, which both import, so the two sides cannot disagree on the encoding.

| Field | Encoding |
|---|---|
| Command | `PROXY` over TCP/IPv4 (`0x21`, `0x11`); `LOCAL` and IPv6 are refused |
| Addresses | the guest's source and the S3 address it dialled; the source must be a routable IPv4 address with a port |
| `0xE0` | contract version, one byte; required, first |
| `0xEA` subtype `0x01` | the `vpce-` ID, AWS's own `PP2_TYPE_AWS` / `PP2_SUBTYPE_AWS_VPCE_ID` |
| `0xE1` | the VPC ID |
| `0xE2` | the endpoint owner's 12-digit account |
| `0xE3` | version 2 only: the policy generation, 8 bytes big-endian, never zero |
| `0xE4` | version 2 only: the endpoint policy document, 1 to 20,480 bytes |

The header is at most 32 KiB, every TLV its version defines is required exactly once, and unknown TLV types are skipped so a later version can add optional fields.
A version 1 header carrying either policy TLV is refused.
A version this gate does not speak closes the connection and logs `unsupported_version`; it is never served under a guessed identity.

The listener speaks contract versions 1 and 2.
A version 1 session is the endpoint's default policy, which allows everything, so only identity policy decides.
A version 2 session carries the endpoint policy, parsed once per connection and evaluated as a second gate: a request is served only if identity policy and the endpoint policy both allow it, at the request check and for each key of a `DeleteObjects` batch.
It applies to every principal, including a configured service account that skips identity policy, as AWS applies an endpoint policy to every caller.
On these requests the gate also supplies `aws:PrincipalArn`, a user's ARN or a role session's role ARN, the only key a statement can use to name a user or role.
A policy the gate would not enforce as written (it does not parse, names a user or role as `Principal`, or has an operand or variable the evaluator cannot read) closes the connection, as an unknown version does.

The two sides are released independently, so the cluster feature level reports this listener only once every gate reads version 2, and an endpoint exists only at that level; the proxy then sends version 2 on every session.
Rolling a gate back below the listener is safe only after the feature level's drain check reports no endpoint in use.
The contract is retired by removing the version from both sides after the feature level that introduced its successor is finalised.

## S3 surface

| Area | Operations |
|---|---|
| Service | `ListBuckets` |
| Buckets | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListObjects` / `ListObjectsV2` |
| Objects | `PutObject`, `GetObject` (incl. `Range`), `HeadObject`, `DeleteObject` |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads` |

S3 overloads one method and path across several operations and distinguishes them by query string, which chi cannot match on, so the split is explicit in `routes.go`: `?partNumber` selects `UploadPart` over `PutObject`, `?uploadId` selects the multipart completion and abort handlers, and `?uploads` on a bare bucket selects `ListMultipartUploads` over `ListObjects`.

Uploads are keyed by upload id alone, so `ListMultipartUploads` scans the whole multipart table and filters on the bucket rather than reading a prefix. In-flight uploads are few and short-lived, which is what makes that affordable. Without the listing an abandoned upload could not be found at all — aborting one needs its bucket, key and upload id — so its parts held storage nothing could attribute to anyone. `DeleteBucket` still checks only for objects, so an upload in flight does not block a bucket delete; the parts are reachable now, but sweeping them is the caller's job.

Deleting a bucket is authorized by the middleware's account comparison alone. The handler once re-decided it on the access key that created the bucket, which meant no other user in the account could remove it and no service credential ever could. Config-declared buckets are refused outright instead, to every caller: they hold the deployment's own state and are shared by every account, and a service credential is exactly the caller the ownership check waves through.

`ListBuckets` carries one extension to the AWS surface. A config-defined service account may send `X-Predastore-Owner-Account-Id` to list the buckets of a named account instead of its own. Such a credential can already open any bucket it can name — `ConfigProvider` marks it `SkipPolicyCheck` and the ownership check short-circuits on that flag — so this adds enumeration, not access, and it is what lets an external control plane discover what a tenant owns in order to delete it.

Three properties make that safe, and each has a test. The header is ignored, not refused, for any other caller, so it cannot be used to tell a real account from an invented one. There is no value meaning "every account": the owner must be a single 12-digit account id, and anything else is `InvalidArgument` rather than an empty listing, because a caller tearing an account down would read empty as "nothing to delete". And a request that somehow reaches the handler with no account at all is refused before the bucket table is read.

## How the gate names things in global state

The meta replicas are a plain key-value store. The table taxonomy is the gate's alone: it composes `<table>/` onto the front of every key it stores and strips it back off every key it scans.

| Key | Value | Purpose |
|---|---|---|
| `objects/<32-byte object hash>` | gob `ObjectToShardNodes` | Placement and object size, for retrieval |
| `objects/arn:aws:s3:::<bucket>/<key>` | the 32-byte object hash | Listing. A page without a delimiter scans from its cursor for at most `max-keys` + 1 rows; a page with a delimiter still scans every row under the prefix |
| `objects/deleted-epoch:<hex object hash>/<16-hex-digit epoch>/<bucket>/<key>` | gob `DeletedObjectInfo` | Delete record of a failed scoped shard delete, one row for each generation. N writes these rows and reads them. N-1 does not read them |
| `objects/deleted:<bucket>/<key>`, `objects/deleted:#<hex object hash>/<bucket>/<key>` | gob `DeletedObjectInfo` | Delete record of a failed shard delete from N-1. N-1 writes and reads only these rows. N reads them and does not write them |
| `objects/part:<uploadID>:<part>` | gob `ObjectToShardNodes` | Placement of an in-flight multipart part |
| `buckets/<name>` | gob `BucketMetadata` | Ownership, region, public flag |
| `multipart/<uploadID>` | gob `UploadMetadata` | The upload itself |
| `parts/<uploadID>:<part>` | gob `PartMetadata` | One uploaded part, zero-padded to 5 digits so scans sort |

The object hash is `sha256("<bucket>/<key>")` and is not valid UTF-8, which is why keys travel the wire as `[]byte` and why snapshots are written as length-prefixed frames rather than JSON.

---

# 6. The meta plane

`internal/meta` is one Raft replica plus the client that reaches replicas wherever they run. A process running several replicas builds one `Server` per node, each with its own rpc server and connection pool, so a replica never learns that it has siblings.

<p align="center">
  <img src="assets/meta-replica.svg" alt="Predastore meta replica: the client prefers the cached Raft leader and follows not-leader redirects; the replica answers key-value opcodes over rpc streams and carries Raft's own traffic over the same streams, applying committed commands into a badger FSM while bolt holds the Raft log and a file store holds snapshots" width="900">
</p>

## Raft over rpc streams

Raft has no port of its own. `RPCStreamLayer` implements `raft.StreamLayer` over the same rpc plumbing everything else uses: an outbound raft connection is a stream opened with `OpRaftDial`, and an inbound one is a stream the handler hands to the layer via `Deliver`, which then holds it open for the connection's lifetime. Raft advertise addresses are node-identifying strings — `node-5` — which the dial function parses back into a node id and routes through the resolver. There is no `raft_port`, no separate TLS setup for consensus, and no second socket to firewall.

One pool serves both directions: the client dials peers from it and the rpc server donates the connections it accepts back to it, so a connection carries raft traffic whichever end opened it.

## Two databases

| Store | Backend | Holds |
|---|---|---|
| Raft log + stable store | bolt, `raft.db` | The ordered command log, current term, vote, cluster configuration |
| FSM | badger, `badger/` | The actual key-value state, written with `SyncWrites` on |
| Snapshots | files, `snapshots/` | Point-in-time FSM state for log compaction and replica catch-up |

Bolt stores *how to build the state*; badger stores *the state*. That split is what the Raft protocol requires.

Snapshots are a stream of length-prefixed frames in key order, behind a four-byte marker: big-endian `uint32` key length, key, big-endian `uint32` value length, value. Text encodings are unusable here, because object rows are keyed by a raw sha256 and JSON rewrites every byte that is not valid UTF-8 to U+FFFD, silently losing the row on restore.

Capturing a snapshot takes a badger read transaction and returns, so it costs microseconds and does not block `Apply`; the store walk happens in `Persist`, which raft runs on its own goroutine. Restoring merge-joins the stream against the local store and writes only what differs, so a replica never passes through an empty window and an interrupted restore converges when it is re-run. Snapshot wins every comparison, which needs no policy behind it: a snapshot is a prefix of the committed log, and raft truncates any divergent suffix before `Restore` is called.

Two older formats still restore, by clearing the store and rewriting it — the legacy JSON map, and an unsorted frame stream from before the marker existed — so a node upgraded on top of an old store still starts and a follower can take a snapshot from a leader on the previous version. That path clears with badger's own bulk clear only when it has decoded its first row, so a legacy snapshot that fails to decode returns an error and leaves the store as it was, and it rewrites through a `WriteBatch`, because a single transaction is capped and a metadata set that outgrew that cap used to make every snapshot permanently unrestorable, on every replica at once.

Commands are JSON: a type (put, delete, swap or transaction), a key and a value, all binary-safe.

### The applied index

The replica keeps one row of its own in badger, `\x00raft/applied`. Its value is the four bytes `PDAI`, a version byte and the big-endian raft index of the last entry whose writes the store holds. Every badger commit that the FSM makes writes this row in the same transaction as the entries it publishes: the shared transaction of a batch, and the transaction of each conditional `CommandTxn`. A batch that publishes no write does not commit, so the row then stays as it was.

On start, the FSM reads the row. When raft replays the log against the store, an entry at or below the row is already in the store, so `ApplyBatch` skips it and answers nil, because no caller waits for a replayed entry. An entry is therefore applied once, and a conditional entry or a `Swap` is never evaluated again against a state that already includes it. A store from a release before the row has no row, and the FSM applies every entry against it, then writes the row on its first commit. A row that does not decode stops the start with an error, and the replica skips nothing.

The shared transaction keeps room for the row. Before it stages an entry, it counts the entry and the row against badger's transaction limits, and it commits first when both would not fit. So an entry that did not fit goes into the next commit, which names it, and replay does not skip it.
The count is badger's own: the key, the value, and 12 bytes of overhead, but only a 12-byte pointer for a value at or above the value threshold, which badger keeps in the value log.
So the FSM refuses an entry that badger alone accepts only when the row's own 38 bytes do not fit beside it.

A failure to read the store while the FSM evaluates an entry, such as a `Swap` or a check of a `CommandTxn`, stops the process.
It is not the entry's answer, because another replica can read and apply the same entry, and a later commit would then name an index past an entry that this store does not hold.
The restart applies the entry again.
A failed check, a malformed transaction and a refused write are the same on every replica, so they stay answers.

A committed entry that this release cannot read also stops the process: an entry that does not decode, one with no `type` or a `null` type, or one whose type this release does not know.
A replica that can read the entry applies it, so answering it and moving on would leave the stores different.
The batch is discarded, so the applied row stays below the entry. Raft does not replay the log at start: it gives the entry to the FSM again once it knows the commit index, which a leader learns only with a quorum of running voters and a follower learns from the leader.
The panic names the index and the type, and never the key. It states the fault only; the cases below decide the recovery.
Recovery depends on the cause:

- An entry that a later release reads: upgrade the replica. It applies the entry from its own log, so it needs no snapshot from another replica, but it applies nothing until a quorum of voters runs again. If a quorum stops, the meta plane is down until enough voters to form a quorum run the later release.
- An entry that only this replica cannot read, because its copy of the log is damaged: rebuild the replica from an empty data directory, as the `node-rebuild` stress scenario does. The leader sends it the log or a snapshot. This needs a healthy quorum.
- An entry that no release can read: every replica that reaches it stops, the leader included. No procedure passes the entry yet, so the meta plane stays down. A leader on a release before this stop answers such an entry with an error and applies the entries after it, so writes at or after the entry may have been acknowledged; any recovery must keep them.

A replica stop does not make a new command type safe to propose. A new type is proposed only after every voter runs a release that reads it, or under a recorded hard-cutover exception. New semantics take a new command type, never a new field on an old one, because an older replica ignores a field it does not know.

No command writes a key under `\x00raft/`. The FSM, the replica's handlers and the client refuse a put, delete, swap or transaction that names one, and a transaction cannot check one. `Get`, `LeaderGet` and every scan, the empty prefix included, never return the row.

A snapshot holds the row at the index that it captured. A restore writes and flushes every data row first, then writes the snapshot's row in its own transaction, or removes the local row when the snapshot has none, and then sets the in-memory index from it. So the index moves back when raft restores an older snapshot. A restore that fails after a write can have reached the store stops the process, as a failed commit does, and the next start restores the snapshot again before it skips any entry. A failed commit of the applied row, or of its removal, also stops the process, because the row can be on disk and name the newer index. A restore that fails before it sends a write returns an error, so raft can restore an older snapshot.
The restore decodes the snapshot's row when the stream reaches it, which in a sorted stream is before any data row except the rare key below `\x00raft/`.
A restore that fails before a write can have reached the store returns an error and changes nothing, so raft tries the next older snapshot on start.

## Client behaviour

Reads try the cached leader first, then every replica, and only report not-found once every replica has answered not-found — a replica that has not applied the key yet must not be mistaken for absence. A follower read may be stale.

Writes go through the leader. A replica that cannot commit answers `not-leader` with the leader's advertise address when it knows one, and the client goes straight there; otherwise it rotates to the next replica after a short pause while an election settles. Attempts are bounded at `MaxRetries × len(replicas)` (default 3 per replica). A 10s per-attempt timeout is layered on the caller's context as a fallback; the caller's own cancellation still wins.

A replica that fails an attempt is ordered last for 30s, so one unresponsive replica costs a deadline once rather than once per operation. This orders replicas and never removes one: reads consult every replica before giving up and writes rotate through all of them, so a stale mark changes how long an answer takes and not which answers are reachable. A replica whose process is stopped but whose transport is still up is the case it exists for — the stream opens, nothing comes back, and only the deadline tells the two apart. The mark clears on the replica's next success, and a caller cancelling its own request marks nobody.

## Tuning

Defaults applied in `meta.New`, over hashicorp/raft's own:

```go
HeartbeatTimeout:   1000 * time.Millisecond
ElectionTimeout:    1000 * time.Millisecond
CommitTimeout:      50 * time.Millisecond
SnapshotInterval:   120 * time.Second
SnapshotThreshold:  2_000_000
TrailingLogs:       2_000_000
LeaderLeaseTimeout: 500 * time.Millisecond
LeaderTimeout:      30 * time.Second   // how long the gate barrier waits
```

### Sizing the snapshot knobs

`TrailingLogs` is how far a replica may fall behind and still catch up by replaying the log rather than taking the whole keyspace. raft's default of 10240 covers about 5,100 object writes, so almost any real outage became a full snapshot install. Two million covers roughly a million object writes for about 566 MB of bolt log — and bolt files never shrink, so that is a permanent floor on `raft.db`, not a peak.

`SnapshotInterval` is a *check* interval, not a snapshot frequency. `runSnapshots` wakes on a randomised 120–240s timer and only takes one when `lastIndex - lastSnapshotIndex >= SnapshotThreshold`. At the default threshold a cluster writing 100 objects a second snapshots about every five and a half hours.

### The raft log store and the apply path

The log is opened with bolt's freelist unsynced and kept as a map (`openLogStore`). A snapshot truncates up to `TrailingLogs` entries from the front of the log, which frees that many pages, and a synced freelist is rewritten in full on every commit: on prod that cost about 1 MB per one-entry commit, around 80 MB/s per node at 80 commits/s. Unsynced, bolt rebuilds the freelist by scanning the file on open, and the file format is unchanged, so an older build opens it and syncs its freelist again.

The FSM takes committed entries in batches (`ApplyBatch`), and applies each batch in one badger transaction, so a batch costs one synced commit rather than one per entry. Entries still apply in order and a `Swap` sees the writes before it in its batch. A failed commit stops the process: badger may have applied part of it. A restart restores the snapshot, if there is one, and replays the log after it, and the replay applies only the entries after the applied index (see *The applied index*). That stop takes the gate and blob roles in the same process down with the replica.

**`SnapshotThreshold` is the knob that decides cost, and it should be set to about the row count.** A snapshot writes the whole store, so at `N` rows of `r` bytes it costs `N × r` and is taken once per `T` entries: each write carries an amortised `N × r / T` on top of its own `r`, making write amplification `N / T`. A threshold well below the row count is amplification; one well above it is a longer replay after a restart, which is far cheaper. The default suits the few-million-object clusters this is built for. **A store an order of magnitude larger needs it raised** — at a billion rows, a threshold of two million is roughly 500× amplification against a store of a few hundred GB.

The log peaks at `TrailingLogs + SnapshotThreshold` entries before compaction, so the defaults imply about 1.1 GB. The snapshot store retains two (`NewFileSnapshotStore(..., 2, ...)`), so a large store also costs twice its size on disk in snapshots.

Bootstrap is attempted by every replica. The peer set is identical across them, so the attempt is idempotent and an already-bootstrapped cluster reports `ErrCantBootstrap`, which is ignored.

Shutdown closes the transport first — a replica that has lost quorum then fails fast rather than blocking on elections — then raft (bounded at 5s, so an unreachable quorum cannot hold the stores open), then bolt, badger and the pool.

---

# 7. The blob plane

`internal/blob` is the shard service: a thin rpc server over `internal/blob/engine`, plus the client the gate uses to reach blob nodes by node id.

A blob node knows nothing about S3. Its vocabulary is a **key** (32 opaque bytes) and an **index** (`uint32`), naming one **value** it stores. That the key is an object hash and the index a shard number is the gate's business.

## Wire protocol

`Request` is the JSON header: key, index, size for puts, `RangeStart`/`RangeEnd` for gets where `-1` means unset, and a **write epoch** naming which generation of the value the caller means. `Response` is a newline-terminated JSON envelope; a get streams `BodyLen` body bytes after it. Three error codes carry protocol meaning — `not-found`, `store-full` and `not-prepared` — and anything else in `Err` is an opaque message. `store-full` is translated back into the engine's own `ErrStoreFull` on the client side, so capacity backoff upstream matches the same error either side of the wire.

A put also reports `PoolNearFull` on success, so callers can back off before a write is ever outright rejected.

**A write is two operations.** `Put` makes the value durable but invisible; `Commit` publishes it and `Abort` discards it. A get names the epoch it wants, and a node holding another generation answers `epoch-mismatch` rather than bytes — see §8. `Stat` answers what generation a node holds without its body, which is what repair asks of every position it owns. Its `suspect` flag reports that a read of that generation failed authentication on the node, so repair rebuilds it (see *Repair*).

**A held put** sets `Hold` in its header.
The node registers the writer in memory and sends a `Registered` envelope before it reads any body byte, and the client sends the body only after that envelope.
A node that predates `Hold` sends no `Registered`, so the client stops waiting after the envelope bound, resets the stream with no body sent, and reports `ErrHoldUnsupported`.
The final envelope carries `Holding`, which confirms that the prepared row is held.
The age sweep drops an unheld prepared row that is older than 30 minutes, and it never drops a held one: only `Commit`, `Abort` or `DeleteEpoch` removes it.
An `Abort` that arrives while the held writer is still open answers `Open` and cuts off the body, and the writer then ends with no prepared row.
An `Abort` that arrives while the writer is closing waits for the close, drops the row, and answers without `Open`.
A held writer that ends without its whole body prepares no row, so a crash at any point leaves nothing for the sweep to keep.
A held body that stops arriving for `BodyIdleTimeout` ends the write.
A second `Abort` then answers without `Open`, which tells the caller that no row is left.

`DeleteEpoch` removes the live, prepared and retained rows of one epoch and no other generation.
Epoch zero is valid and names a row written before epochs, since no write path makes another row at zero.
A node that predates it closes the stream with no envelope, which the client reports as an error.

## The engine

<p align="center">
  <img src="assets/blob-ondisk.svg" alt="Predastore blob node on-disk format: a data directory of state.json, a badger index, append-only .seg segment files and their .idx sidecars; each segment is a 14-byte header followed by fixed 8240-byte fragments; each fragment is a 32-byte plaintext header, an 8192-byte AES-256-GCM ciphertext body and a 16-byte tag bound to the value's position" width="900">
</p>

### Data directory

```
<data dir>/
├── state.json                 # monotonic counters + the data dir's crypto identity
├── keycheck                   # evidence that the master key is the key that sealed this store
├── db/                        # badger: key‖index → extent, plus tombstones
├── 0000000000000000.seg       # append-only fragments
├── 0000000000000000.idx       # reverse sidecar: what was ever allocated in that segment
└── ...
```

### Segments

A segment is a 14-byte header — magic `S3SE`, version 1, a 32-bit flags word, 4 reserved bytes — followed by a sequence of fixed 8240-byte fragments. The previous pre-encryption magic is rejected outright by `openSegment`: there is no in-place migration, and an operator upgrading must start with a fresh data dir.

A segment grows to `maxSegSize` (4 GiB), at which point `flagFull` is set in its header and the store rolls to the next segment number. The flag is cached in memory so the hot append path does not pay a `ReadAt` for it. One oversized value is let through on a fresh segment, so a pathological size still makes progress rather than looping.

### Fragments

| Part | Size | Contents |
|---|---|---|
| Header | 32 B | `fragNum`(8) ‖ `valueNum`(8) ‖ reserved(4) ‖ `size`(4) ‖ `flags`(4) ‖ reserved(4) |
| Body | 8192 B | AES-256-GCM ciphertext; GCM is length-preserving, so the ciphertext is exactly the plaintext length |
| Tag | 16 B | GCM authentication tag over ciphertext and AAD |

`size` is the logical payload length in this fragment; bytes past it are zero-padding inside the ciphertext. `flags` carries `flagEndOfValue` on a value's last fragment. The header is plaintext because a reader needs `fragNum` and `valueNum` *before* decryption to rebuild the nonce and AAD — but both feed the AAD, so tampering with them fails the tag.

### Extents and the index

A value of logical size `S` occupies `⌈S / 8192⌉` fragments in a contiguous extent within one segment. The index maps a 36-byte key (`key`(32) ‖ big-endian `index`(4)) to a 32-byte extent record:

| Field | Meaning |
|---|---|
| `SegNum` | Which segment holds it |
| `Off` | Byte offset of its first fragment within that segment |
| `PSize` | Physical size on disk, `fragCount × 8240` |
| `LSize` | Logical size the caller sees |

**The index row is the only authority on what is readable.** Bytes on disk with no index row pointing at them are invisible and reclaimable. This is the linearisation point for readers, and it is what makes an overwrite safe: `Append` reserves and writes into a new extent while the old one keeps serving, and the swap happens in the single index transaction `Close` performs.

### The .idx sidecar

Every reservation also appends a 52-byte row — `Off`(8) ‖ `Key`(36) ‖ `PSize`(8) — to the segment's `.idx` file. It is what compaction enumerates, so it can list a segment's extents without scanning the whole index. The rows are hints: each must be back-checked against the index, which is the authority on what is live. The sidecar is fsynced *before* the index commit, so every index-committed extent is already findable in `.idx` and a segment drop cannot lose live data. A torn trailing row is a crash mid-append, and that extent never went live.

### Write path

`Append` runs one short critical section under `store.mutex`, then does no data I/O under a lock at all:

1. Check the free-space watermark. Below `full` (5%) reject with `ErrStoreFull`; below `nearfull` (15%) kick an immediate compaction pass but accept the write. A `statfs` error is treated as permissive — a monitoring hiccup must not take writes down.
2. Compute the fragment count.
3. If the allocation would cross the durably-reserved `fragNum` high-water, advance it and fsync `state.json`.
4. Find or roll to a segment with room, and `Truncate` it to pre-allocate the extent.
5. Append the `.idx` row, take a segment reference, and hand out the extent, `valueNum` and `fragNum` range.

The writer then owns a disjoint byte range. It assembles fragments into an in-memory window (32 fragments, ≈ 256 KiB per active stream), seals each body in place under the shared AEAD, and issues one `WriteAt` per window. Concurrent writers may write the same segment file because their extents are disjoint: POSIX `pwrite` is atomic for non-overlapping regions, Go's `WriteAt` is safe for concurrent use, and the stdlib's GCM is safe for concurrent `Seal`/`Open`.

`Close` then flushes, fsyncs the `.seg`, fsyncs the `.idx`, and commits the index row — in that order. A failure before the last step leaves the previous value intact and the new bytes as dead space. There is no distinct "failed" state on disk: the absence of an index row is the authoritative signal, and readers treat it identically to "never written".

`statfs` is throttled to at most once a second, and forced regardless once 64 MiB of extents have been reserved since the last measurement, because free space tracks write volume rather than wall-clock time.

### Read path

`Lookup` reads the index row, opens the segment, takes a reference, and returns a reader over the extent. Reads are batched into the same 32-fragment window, opened in place, and a failed tag returns `ErrIntegrity`. The reader satisfies `io.ReaderAt`, so a ranged get is served by reading only the fragments the range touches. The caller must close the reader to release the segment reference.

### Delete and tombstones

`Delete` removes the index row and writes a tombstone in one transaction, so the dead-space hint can neither precede nor outlive the deletion. A missing key is not an error, which keeps deletes idempotent. `Delete` removes every generation at the position. `DeleteEpoch` removes only the rows of the epoch it names, so a stale delete cannot remove a generation that a later write made. An overwrite tombstones the extent it supersedes inside the same commit transaction.

Tombstones are keyed by physical slot — `'d'` ‖ big-endian `segNum` ‖ big-endian `off`, 17 bytes — with the dead byte count as the value. Keying by slot rather than by object key is what lets one key die repeatedly: on delete, on overwrite, and on a relocation that lost its race. They only accelerate compaction's candidate selection and are never consulted for correctness.

### Retained generations

A `Commit` that publishes a prepared row over a live row **at the same epoch** does not demote anything. The live row keeps serving that epoch, so no record can name the bytes it displaces, and the same transaction writes a tombstone for them. A reader that already holds them keeps its segment reference, and compaction does not detach a segment with references. This is the case of a retried write and of a repaired shard.

A `Commit` promotes the new generation and **demotes** the one it replaces to `'r'` ‖ key ‖ index ‖ big-endian epoch, rather than destroying it. Shards are published before the placement record, so a record can name a generation the nodes have already passed, and a demoted generation still answers `LookupAt` for exactly that reason.

A demoted generation is reclaimable only once a publisher reports that its record displaced it. That report is `Release`, and it writes `'R'` ‖ key ‖ index ‖ epoch, a row whose presence is the whole eligibility test. `retainedMaxAge` (5 minutes) then applies to the release, not to the demotion: it is the grace window a reader that already resolved the old generation keeps answering in.

A release row is found only through its retained row, so a shard delete that removes the retained row leaves the release behind. A background sweep on the same 15-second tick reads at most 10,000 release rows, resuming where the last sweep stopped, and drops any whose retained row is gone, checking again inside the deleting transaction so a retained row written back in between keeps its release.

Age cannot decide on its own. A writer that commits and never publishes starts no clock anybody can see, the gate's PUT path carries no total deadline, and reclaiming such a generation destroys an object every client was acknowledged for. The sweep therefore reclaims released generations past their grace and nothing else, ever. A release that does not arrive costs disk until the generation is released again, which is the safe direction: failure retains.

The retained row's stamp field is written `math.MaxInt64` and is never read here. It exists for an engine from before the release namespace, which reclaims on that field and will not find the value old enough, so a generation this engine demoted is stranded rather than destroyed by a downgrade.

That is not a rollback guarantee, and there is none. A retained row written before the release namespace existed carries the real stamp its own commit wrote, this sweep holds it because nothing released it, and an older engine then reclaims it on age. Where a live placement record still names such a generation, going back destroys an object every client was acknowledged for, and no scan answers whether one does. A defect found on this engine is fixed forward.

### Compaction

Compaction is always on. Without it, overwrite and delete churn frees nothing and the store fills. The interval comes from `[compaction] interval_seconds`, defaulting to 5 minutes, and a nearfull `Append` kicks an out-of-cycle pass.

Routine passes are paced so reclaim does not take the foreground's latency, and never on every node at once:

- **Staggered start.** Each blob node takes a slot, its position among the cluster's blob node IDs, sorted. A routine pass starts at the next wall-clock instant aligned to the interval plus `slot × interval / slots`, so three nodes on 5 minutes start 100 s apart and a restart keeps its node's phase. The timer re-arms after each pass, so a pass longer than the interval skips to the next slot.
- **Byte-rate cap.** `[compaction] max_bytes_per_sec`, default 64 MiB/s, paid per 256 KiB copy buffer, so a large extent is paced inside itself. Every wait runs outside `store.mutex`.
- **Foreground yield, off by default.** With `yield_latency_ms` set, the pass waits in 100 ms steps while the highest foreground segment read or sync latency of the last second is above it, for at most `max_yield_seconds` (default 60) per pass. Compaction's own reads and syncs are not counted.
- **Urgent reclaim is unpaced.** A pass that starts below the nearfull watermark, or is kicked, runs without pacing or yields. A kick, or the store crossing nearfull, during a paced pass makes the rest of it unpaced and re-arms a fresh pass.
- **The retained sweep keeps its 15 s cadence** through a long pass: it runs between copy buffers once 15 s have passed.
- **Close** finishes the extent in flight unpaced and ends the pass, leaving the segment for a later pass even when that extent was its last.
- **Bounded waits.** One rate wait lasts at most 1 s before the pass checks free space, the full band included, and the retained sweep again.

One cycle:

1. Scan the tombstone namespace and sum dead bytes per segment. Keys carry no namespace prefix, so roughly one hash in 256 starts with `'d'` — the fixed tombstone width is matched before the key is trusted.
2. Select segments whose live fraction is below 70%. The active append segment is never a candidate: it is the relocation destination.
3. Persist `segNum` before dropping anything, so a restart cannot recreate an empty segment at a number this cycle is about to drop.
4. For each candidate, walk its `.idx`, back-check each row against the index, and relocate the rows that are still live.
5. Fsync the index, drop the segment and its sidecar, and clear its tombstones.

Relocation copies extent bytes **verbatim** — `fragNum` rides inside the copied bytes, so the nonce moves with them and is never reissued — then commits the repoint only if the row still holds the extent it copied from. Losing that race strands the copy for good, since only that swap could ever have referenced it, so the loser tombstones its own copy rather than let it pad the destination segment's live count. The copy itself runs without `store.mutex`, so compaction never stalls the write path.

A lost swap does not mean the slot is dead. The commit that won it can have moved the row into another namespace still naming the slot: an overwrite demotes the live row to a retained one, and a commit publishes a prepared row as live. So the slot is read again, in one snapshot across the live, prepared and retained namespaces, and relocated again under whichever row now names it, up to three times. If a row still names a slot after that, the segment is kept and the next cycle retries. A segment with a writer still open in it is deferred before its sidecar is read: the writer reserved its slot before the segment rolled and has not prepared its row yet, so enumerating it would miss that extent.

The index runs with badger's sync writes off, so relocations may still be in the page cache. The index is fsynced before **every** drop, including one whose pass moved nothing because an earlier pass relocated everything and then failed: that is what stops a power loss from reverting the index to a segment that is already gone.

The drop holds `store.mutex` only to take the segment out of the cache and close it. The unlink runs after the lock is released: ext4 frees every extent of a file before `unlink` returns, about 1.6 s for a sealed 4 GiB segment, and every shard write and read on the node takes that lock. The `.seg` is unlinked before its `.idx` and the directory is fsynced, all before the tombstones are cleared, so a failed or interrupted drop leaves either both files, which the next cycle drops again, or the sidecar alone, which the next cycle removes with the tombstones. A failed directory fsync leaves both files gone and the tombstones in place, and the next cycle syncs and clears them. That cleanup only ever touches a number below the active segment, where no append can create one, and it removes nothing but the sidecar. A segment whose `.seg` is present but whose sidecar is missing is reported, never treated as dropped. The live-segment count drops when the `.seg` is unlinked, not when it is detached, so a retried drop counts it once.

### Persistent state

`state.json` holds `segNum`, `valueNum`, `fragNum`, `fragNumHighWater` and `storeID`. It is written atomically and durably: write `state.json.tmp`, fsync it, rename, fsync the parent directory.

`fragNum` uniqueness across crashes is preserved by batched high-water reservation. On `Open` the store advances `fragNumHighWater` by 1 048 576 and fsyncs; `Append` then hands out values freely below the high-water without touching disk, and only an allocation that would cross it costs another fsync. On recovery, `Open` resumes at `fragNumHighWater`: the unflushed window from before the crash is sacrificed, because a rewound `fragNum` would reissue a nonce under the same key, which breaks GCM catastrophically. At most a million values of nonce space are wasted per crash, against a 2⁶⁴ budget.

The first save also locks in a freshly generated `storeID` before any fragment can be sealed under it — a crash first would generate a different one and orphan everything written under the old.

### Shutdown

`Store.Close` marks the store closed under the mutex, joins the compactor without holding it (an in-flight cycle takes the same mutex), saves state, waits for every segment's references to drain, then closes the segment file descriptors and the index.

---

# 8. Object lifecycle

## PUT

<p align="center">
  <img src="assets/put-path.svg" alt="Predastore PUT object path: the gate authenticates the request, hashes the object name, places it on the ring, Reed-Solomon encodes the body into data and parity shards, streams each shard to its blob node where it is sealed and committed, then records the placement in the Raft metadata plane" width="900">
</p>

The gate authenticates, resolves the bucket, unwraps `aws-chunked` framing if the client used it, and takes the object size from `Content-Length` or `X-Amz-Decoded-Content-Length`. A request that declares no length is rejected with `MissingContentLength`: the splitter needs the size up front, and it is what placement records.

Every PUT mints a **write epoch**: 44 bits of milliseconds since 1970, 10 bits of gate node id and a 10-bit per-millisecond sequence, with zero reserved as invalid. It is stamped on every shard of the stripe and into the placement record, so the record names one generation and a shard either belongs to it or does not.

It is compared only for equality, so uniqueness is the one property the shard path needs — an epoch that repeated for a key would let a surviving shard of an older write be accepted as current. The node id is what makes that structural rather than probabilistic: two gates minting in the same millisecond cannot collide. The minter never returns a value at or below one it has already returned, so a clock that steps backwards costs sequence numbers rather than safety.

The milliseconds are the object's `LastModified` on every surface that serves one, which is why the record spends its eight bytes here rather than on entropy plus a separate modification time. A version 1 record's epoch is random and carries no time; those objects report the zero date, and always will.

The body is split into `K` data shard buffers in memory and each is streamed to its node in parallel. Parity is encoded into buffers of its own and written from those. It is deliberately not piped from a single encoder: one parity node refusing would close its pipe, fail the encode and take every other parity write down with it, losing all redundancy at once rather than one shard of it.

**The write is two-phase.** Each shard put makes the bytes durable and invisible. The shards are published first, then the placement record lands in raft, which is the commit point and the moment the object's new generation becomes visible. That order is what stops a record naming shards that have not landed; the reverse is possible — a record can name a generation the nodes have already passed — which is why a commit demotes rather than destroys. A reader that finds a prepared-but-unpublished shard at the record's epoch, behind an older live generation, completes the commit itself, so an interrupted overwrite costs a round trip rather than the object. A get that names an epoch at a key with no live generation does the same, so a record that lands before its first shard commit is readable while the prepared row exists.

Swapping the record rather than writing it tells the writer which generation it displaced, and the writer then **releases** exactly that one on every node behind the old record, including the handoff holder. A generation nobody released is never reclaimed: see §7, *Retained generations*. A writer that does not reach the release leaves disk held, which is the direction to fail in.

**A write is acknowledged only once `K` shards are committed at its own epoch.** Landing is not enough: a shard that landed and then lost its commit is readable only until the age sweep drops its prepared row, and on production a chunk was acknowledged with one of its two landed shards committed and failed its read-back seconds later. `commitShards` counts a shard as durable when its commit published, or answered overtaken by a newer generation the node holds behind it — the engine proves that with the retained row, and answers not-prepared for an epoch it never held. Below the floor the write fails with `InternalError` before any record, parts row or listing key names it, and its still-prepared shards are aborted. A shard that did commit stays live under an epoch nothing names; it costs disk and never a read. PutObject, CopyObject, CompleteMultipartUpload, UploadPart and UploadPartCopy all hold to it, and an empty object, which stores no shards, has no floor.

The floor is `K` with degraded writes, so one node down does not refuse writes; the response then carries `X-Spx-Degraded-Write: <n>`. Setting `rs.degraded_writes = false` restores a full-width floor. `K` is the floor and not `K+1`, because any `K` of the `K+M` reconstruct — at RS(2,1) one more buys no redundancy and would refuse three quarters of writes with a single node down.

The floor is a **failure threshold, not a target**: the write places every shard it can reach, and the floor only decides whether to fail. At RS(3,2) with one node down the write acks with four shards and one parity unit either way; a floor of `K` rather than `K+1` differs only at two nodes down, where it accepts with no parity instead of refusing.

The gap it opens is closed by repair, below — but only when the missing node returns, because repair rebuilds a shard onto its owner. At RS(2,1) an object written while a node is down holds no parity for the duration of the outage. That is the accepted position: a lost node is repaired and returned to service, and refusing writes meanwhile is the worse failure.

**Hinted handoff narrows that gap to nothing.** A shard its owner will not take is written to the node one step off the end of the stripe on the ring instead of being given up on, and the response carries `X-Spx-Handoff: <n>`. The stripe is then complete and the object is as redundant as a full-width write; what is outstanding is only that the shards are not all where the record says. A position is handed off or missing, never both, so `X-Spx-Degraded-Write` still counts exactly the shards that landed nowhere.

The holder is **derived and never recorded**: it is `ring.Nodes(hash, K+M+1)[K+M]`, which the read path and repair both recompute, so there is no hint to store, replicate or lose. Handoff is skipped when the cluster has no node to spare — placing a second shard of one object on a node that already holds one of its shards is not redundancy. Commit and abort follow the shard to wherever it actually landed: committing at the owner would be told the shard was never prepared, and the holder's copy would stay durable, unreferenced and unreadable.

Only the shards that actually prepared are committed or aborted. Committing a shard that never prepared is refused, and aborting one would ask a node to discard the generation it is still serving.

**The ETag is the MD5 of the body**, teed off the read that feeds the shards. A plain body sent with both `X-Spinifex-Ebs: true` and `x-amz-checksum-crc64nvme` is checked with CRC64NVME instead and hashed with nothing else: viperblock sends its chunks that way and never reads an ETag, and MD5 is the slowest step of the write at about 400 MB/s a core. The object's ETag is then the CRC64NVME and the length in hex, not an MD5; a mismatch fails the write with `BadDigest` before the commit, and a malformed value with `InvalidDigest` before any shard is placed. Either header alone, or an `aws-chunked` body, keeps the MD5 ETag, because the AWS CLI sends CRC64NVME by default.

If any node reported nearfull pressure, the response also carries `X-Predastore-Pool-Pressure: nearfull`; a node that rejected outright surfaces as 507 `InsufficientStorage`.

**Ordering matters.** Shards are durable before metadata. A crash between the two leaves shards nothing points at (reclaimed by compaction only once something tombstones them — see §15); a crash the other way round would leave metadata pointing at data that was never written.

## GET

<p align="center">
  <img src="assets/get-path.svg" alt="Predastore GET object path: the gate reads the recorded placement from the Raft metadata plane, fetches the data shards from their blob nodes in parallel, joins them, and falls back to fetching parity and Reed-Solomon reconstruction when a shard is missing or fails its integrity check" width="900">
</p>

The gate reads the recorded placement — which carries the object size and the write epoch the join needs — fetches the data shards, and joins them. Only if the join fails does it refetch including parity and reconstruct. Parity is never touched on the happy path.

Every shard read names the record's epoch, and a node holding another generation answers `epoch-mismatch`. **The gate counts that exactly as a missing shard.** This is what makes a half-completed overwrite a reconstruction rather than an object spliced from two generations: the stale shards are not eligible to be joined, so the read either rebuilds the object the record names or fails, and never returns plausible nonsense. A read that had to reconstruct says so in `X-Spx-Degraded: <n>`, which is a cost report and not an error.

A shard its owner cannot serve is looked for on the handoff node before it is counted as failed, since a write that could not reach the owner will have put it there. That is one fetch against `K` fetches and a decode, so it is worth asking even though the usual answer is no — and without it a handed-off stripe would be complete only on paper, the bytes existing where the gate never looks.

A `Range` request is served straight from the data shards when it is short: the data shards hold the object's own bytes, so a range inside one block is a single ranged shard read, and a range no longer than one block and no longer than 2 MiB that crosses block boundaries is one ranged read per block it touches, each read into a buffer of exactly its length. Each read is hedged: if its owner is slow or fails, the same range is rebuilt from the other shards. These direct reads do not send `X-Spx-Degraded`; the reconstruction metric counts them. A read that still fails, or a longer range, goes to the stripe reader, which reads the stripes the range touches and drops the lead-in.

## DELETE

The gate loads the placement and sends `DeleteEpoch` at the placement's `WriteEpoch` to every shard position in parallel, then removes the placement record and the listing row.
Each shard delete removes only the rows of that epoch, so a write that lands at the same positions after the placement read keeps its shards.
If a shard delete fails, the gate writes a `deleted-epoch:` record before it removes the placement record, and the object still leaves global state.
The record key contains the object hash, the `WriteEpoch`, the bucket and the key, so each failed delete of a distinct generation has its own record.
The record names the nodes and the epoch.
The prefix does not start with `deleted:`, so a reaper from before this prefix never reads the record.
An object written before epochs has `WriteEpoch` zero, and its shards are deleted at epoch zero, which matches only rows from before epochs.

The tombstone reaper sweeps the `deleted-epoch:` records and the `deleted:` records.
The release before the `deleted-epoch:` prefix writes a `deleted:` record only after a failed shard fan-out, so a `deleted:` record still owes its shards.
For each record, the reaper reads the live placement at the record's hash with `LeaderGet`.
When the leader cannot answer, the reaper keeps the record for a later pass, because a lagging follower can name an older epoch.
A live placement at the record's own epoch is the delete still in flight, and the reaper keeps the record for a later pass.
In the other cases, the reaper sends `DeleteEpoch` at the record's epoch to every node the record names.
A live placement at another epoch is a recreate, and the reaper still deletes the record's epoch, because that delete cannot remove the shards of the recreate.
A record at epoch zero, with either prefix, is a failed delete of an object from before epochs.
The reaper deletes its shards at epoch zero, so a recreate at a positive epoch keeps its shards.
The reaper removes the record only when every shard delete succeeds, so a node that was down is reclaimed on a later pass.
A node from before `DeleteEpoch` closes the stream unanswered, and the record stays until that node is upgraded.
Multipart part cleanup keeps the unscoped `Delete`.
An object at the hidden part key of an upload has the same hash as the part, and the cleanup also removes the shards of that object.

## Multipart

`CreateMultipartUpload` records upload metadata. Each `UploadPart` is stored exactly as an object would be — erasure coded and placed — but under a hidden object name, `.multipart/<uploadID>/<key>/<part>`, so an in-flight upload never shows up in a listing. `CompleteMultipartUpload` validates the claimed parts against what was stored, reads the parts back (fan-out bounded at 10 concurrent fetches), concatenates them into a temp file, and writes the result exactly as a single-shot `PutObject` would. Cleanup then drops the part shards and their metadata; it is best-effort throughout, since a cleanup failure must not fail the completion.

`AbortMultipartUpload` runs the same cleanup without writing the object.

---

# 9. Erasure coding and placement

The erasure code is cluster configuration, not a per-request choice, and it has no default: a substituted one would place objects at a width the cluster was never checked against.

```toml
[rs]
data   = 2   # K
parity = 1   # M
```

Validation enforces two rules. `data + parity` must not exceed the number of blob nodes, because placement spreads a stripe over distinct nodes and a narrower cluster fails every write. And `parity = 0` is only legal at `data = 1`: zero parity delegates redundancy to whatever sits under the blob node, which only holds while the object is one shard on one node — striping it wider without parity makes losing any node lose every object.

An object is encoded end to end into `K` data and `M` parity shards with no intermediate chunking. Each shard is roughly `⌈size / K⌉` bytes. Any `K` of the `K+M` shards rebuild the object.

## The ring

`internal/gate/placement` wraps `buraksezer/consistent` with xxhash. Members are named `node-<id>`, so placement resolves straight to the id the blob client addresses. `Nodes(objectHash, K+M)` returns the placement in shard order: shard `i` goes to `Nodes()[i]`.

The ring is a concrete implementation, not a pluggable strategy. Every gate in a cluster must derive the same placement from the same object hash, so there is nothing to swap at runtime.

| Parameter | Value |
|---|---|
| Partition count | 5 |
| Replication factor | 100 |
| Load factor | 1.25 |

The gate records the resolved placement rather than relying on the ring alone at read time, so a ring that has since changed shape does not lose objects placed under the old one. It does check that the recorded shard count still matches the ring's width.

## Repair

`internal/gate/repair` restores shards a blob node owns but does not hold at the generation its object's record names. It runs on every host with a local blob node unless `[repair] enabled = false`; it is what closes the redundancy window degraded writes open, and a cluster running one without the other takes the cost without the repayment.

Rebuild concurrency derives from the CPU count but is capped at 8. Half of a 256-thread host is 129 concurrent rebuilds, which is not a sweep contending with serving but one displacing it, and a rebuild is bounded by disk and peer bandwidth well before it is bounded by goroutines.

**It carries no correctness weight.** A read compares each shard's epoch against the record it has already loaded and discards a stale one whether or not repair has ever run. What repair restores is redundancy — the number of further losses an object survives — and never the object.

**It is a sweep, not a subscription.** Every pass pages through the placement records from the beginning, keeps the ring positions belonging to the nodes it repairs for, asks each holder which generation it has, and rebuilds the ones that disagree. Nothing is derived from a log window: the meta FSM keeps only the index of the last entry it applied, not the commands before it, so there is no index-to-command history to walk and no cheaper question to ask than the records themselves.

| Property | Choice |
|---|---|
| Coordinator | The gate, for the blob nodes in its own process |
| State | An in-memory scan cursor and counters — nothing durable |
| Cheap path | Publish a shard the node already prepared, one round trip |
| Next cheapest | Pull it back from the handoff holder, one transfer and no decode |
| Rebuild | `Reconstruct` from `K` peers, streamed into the put |
| Source shards | All demanded at the record's epoch, never a mixture |

Repairing only the nodes sharing its process settles ownership deterministically: every blob node has exactly one coordinator and nothing has to elect one. Keeping no durable state is affordable because every rebuild is idempotent against the record's epoch — a crash repeats a page rather than losing work, and a durable cursor would need its own consistency argument against a table being written underneath it.

A shard that was handed off is returned rather than recomputed. Repair derives the holder the same way the write path did, asks it for the shard at the record's epoch, streams it to its owner and publishes it there — and only then releases the holder's copy. Emptying the holder first would turn a failed return into exactly the loss handoff existed to prevent.
The release is `DeleteEpoch` at the record's epoch, so a later write that landed on the holder meanwhile keeps its shard.
When the publish discards the owner's copy because the record moved, the holder keeps its copy, because a read of the old record can still need it.
A holder that cannot run `DeleteEpoch` keeps its copy, and the return still counts, because the owner holds the shard. A holder that has nothing, or has another generation, refuses, and the rebuild proceeds as it would have.

Before publishing, the record is re-read and the rebuilt shard discarded if the epoch has moved, so a write that lands mid-repair is not demoted to the generation before it. That window is narrowed rather than closed; what survives it is a shard the next read discards and the next pass rebuilds, which is a wasted repair and not a wrong object.

**A shard that fails authentication on a read is rebuilt at its own generation.** When a get's body fails its GCM tag, the node records the position and the generation, each generation on its own, in a bounded in-memory set of 4096 entries, and a later `Stat` reports that generation `suspect`. Repair's `Stat` names the record's generation, so a corrupt generation that S3 still serves from the retained row, under a newer live one whose writer has not published, is reported too, and repair rebuilds it and reads it back. A `Stat` reports the mark of the generation it is asked about, or of the live one, and clears no other. A full set evicts its oldest entry, and an evicted shard that is still bad enters again at its next failed read. A whole read that authenticates, a delete, or a `Stat` that finds the position absent clears the entry; a ranged read does not, because it can miss the bad fragment. Repair sends a suspect position straight to the rebuild route, because publishing or a handoff copy changes nothing at a generation the node already holds, and then reads the shard back in full, which clears the entry. A peer whose body fails during the rebuild is skipped, and the rebuild tries again from the stripe's other peers; when too few are left, that stripe fails and the pass goes on. A record written under another erasure code than the one repair runs is left alone. The node marks nothing while its store's key is unverified (see *Encryption at rest*), because under another key every shard fails and none may be rewritten. A store created before the key check is never verified, so repair of suspect shards is off on it. A restart forgets the set; the next failed read finds the shard again.

If fewer than `K` peers hold the record's generation, the rebuild fails and says so. Inventing a shard from a mixture of generations would replace a detectable loss with an undetectable one, which is the defect this whole design exists to remove.

---

# 10. Encryption at rest

Every fragment is sealed independently under AES-256-GCM. The 12-byte nonce and 52-byte AAD are deterministic and fully reconstructable at read time from the on-disk header plus per-data-dir state:

```
nonce[0:8]   = BE(fragNum)     // from the fragment header
nonce[8:12]  = BE(storeID)     // from state.json, 4 random bytes per data dir

aad[0:32]    = key             // sha256, already part of the index key
aad[32:36]   = BE(index)       // already part of the index key
aad[36:44]   = BE(valueNum)    // from the fragment header
aad[44:52]   = BE(fragNum)     // from the fragment header
```

What this buys:

- **Confidentiality.** Disk-level access yields ciphertext and tags. Plaintext is never written.
- **Authenticated integrity.** GCM is the sole integrity authority; there is no separate checksum. Disk corruption, tampering and a wrong master key all surface as one `ErrIntegrity`.
- **Key check.** The `keycheck` file is the evidence that the node's master key is the key that sealed the store, which is what lets a failed read count as corruption. Only a new store gets one: `Open` seals it when the directory holds no segment file at all, bound to the store's `storeID`, so a copy from another store does not open. A store that held data before the file existed gets none, because it may have run under another key, and a node alone cannot tell which key its peers use; at open it logs that suspect-shard repair is off on it until a check across nodes can confirm its key. A key that does not open the file leaves the store unverified for good: the file is renamed `keycheck.refused` and kept for diagnosis, because the store goes on accepting writes under that key, so the original key returning later proves nothing. If that rename or its directory sync fails, the store does not open, so it takes no write under that key while a restart could still trust the original check. A file that is damaged leaves the store unverified while it stays so. In both cases reads and writes run as before and no shard is marked for repair.
- **Position binding.** The AAD binds each fragment to its `(key, index, valueNum, fragNum)` slot, so swapping fragments between values — or rewriting the header to claim a different slot — fails the tag.
- **Cross-data-dir defence.** `storeID` enters the nonce, not the AAD, so splicing a fragment from one data dir into another yields a different nonce at read time and the tag fails.
- **Mandatory.** `engine.Open` errors without `WithAEAD`, `blob.New` errors without an AEAD, and `s3d` refuses to start without `-encryption-key`. There is no unencrypted code path.

The master key is one 32-byte AES-256 key per cluster, loaded once at startup by `pkg/masterkey`. The loader is fail-closed on permissions: any group- or other-readable mode is rejected outright with no override, because the master key gates plaintext access to every object cluster-wide and warn-and-allow would put the cluster one ignored log line from a permanent breach. The raw bytes are not retained past `Load` — only the `cipher.AEAD` and a short log-safe fingerprint, `hex(sha256(key)[:8])`.

Every node in a cluster must be given the same key file. Fragments sealed under one master cannot be opened under another. Rotation is out of scope for the current implementation (see §15).

Binaries are built with `GOFIPS140=v1.0.0`, and `bluebottle/pkg/fipsboot` panics at startup if FIPS mode was turned off at runtime via `GODEBUG`.

---

# 11. Security

## TLS

- The S3 API serves TLS 1.3 minimum with a restricted curve preference, from the host's `tls_cert` / `tls_key`.
- QUIC between hosts uses the same keypair, TLS 1.3, and pins the cluster ALPN `mulga-repl-v1`, so a handshake against anything but a cluster peer fails outright. Dialers verify the server certificate against the OS trust store, with no `InsecureSkipVerify` anywhere.
- Raft rides those same connections and needs no TLS setup of its own.

Certificates belong to the host, not the node. That is what TLS can express: SANs carry no port, so two nodes on one machine are indistinguishable to TLS regardless. One keypair therefore serves both the public and the cluster plane; a separate certificate per plane is not implemented, and neither are per-node client certificates (mTLS).

Keeping the cluster plane off the public interface is a bind-address question rather than a TLS one — see [Two planes](#two-planes).

Standalone operators must install the cluster CA into the host trust store before launching `s3d`, or nodes cannot dial each other. Under Spinifex this happens automatically during node bootstrap.

## Authentication and authorization

- **SigV4** on every S3 request, against the URI the client actually signed. Presigned URLs are supported. Global operations are signed against `us-east-1` regardless of the cluster region.
- **Config-defined accounts** in `[[auth]]` are trusted service accounts: the policy table is only consulted for IAM accounts. Each must carry an `account_id`, so buckets it creates land with a real owner.
- **IAM via NATS JetStream KV**, when `[iam]` is configured: access keys, users, roles and policies are read from KV buckets and layered over the config accounts through a chain provider. Predastore serves S3 over HTTP and does not subscribe to any NATS request topic; KV is the only thing NATS is used for.
- **Bucket ownership** is checked cross-account after policy evaluation.
- **Copy sources** are authorized as reads: `CopyObject` and `UploadPartCopy` need `s3:GetObject` on the source, or `s3:GetObjectVersion` when it names a version, under identity policy, the source bucket's ownership and any endpoint policy, as on AWS.
- **Multipart operations** carry their own actions where AWS names one: `s3:ListMultipartUploadParts`, `s3:AbortMultipartUpload` and `s3:ListBucketMultipartUploads`.
- **Public buckets** short-circuit authentication for GET and HEAD only; everything else still requires a signature, and listing buckets is never public.

## Rate limiting

Optional, keyed by `(account, action)` with per-action overrides, answering `SlowDown` with 503 when a bucket is exhausted.

---

# 12. Configuration

One TOML file, meant to be identical on every machine. The settings only the local machine cares about — its data directory, TLS identity and encryption key — may be omitted and supplied as flags instead. Parsing is strict: an unknown key is either a typo or a setting this build dropped, and both otherwise read as configured until something misbehaves.

```toml
version = 1                     # no migrations: a format change rewrites the file
region  = "ap-southeast-2"      # required; the credential scope requests are compared against

[rs]
data   = 2
parity = 1
# degraded_writes = true        # on; false refuses a write short of K+M. See §8
# hinted_handoff  = true        # on; false leaves the stripe short instead. See §8

# [repair]                      # restore shards held at a stale generation; see §9
# enabled          = true       # on wherever the host runs a blob node
# workers          = 0          # concurrent rebuilds; zero derives one from the CPU count, capped at 8
# page_size        = 0          # placement records per scan request; zero defaults to 512
# interval_seconds = 0          # gap between passes; zero defaults to 300

# [compaction]
# interval_seconds = 300        # zero uses the engine default; compaction is never off
# max_bytes_per_sec = 0         # routine-pass copy cap; zero defaults to 64 MiB/s
# yield_latency_ms  = 0         # foreground latency a routine pass yields to; zero is off
# max_yield_seconds = 0         # most one pass yields; zero defaults to 60

# [meta]                        # raft log retention on the metadata plane
# snapshot_interval_seconds = 0 # zero defaults to 120
# snapshot_threshold        = 0 # zero defaults to 2000000; size it to the row count
# trailing_logs             = 0 # zero defaults to 2000000; decides replay vs snapshot install

[[host]]
id   = 1
addr = "10.11.12.1"             # what peers dial; no port — nodes carry those
# bind_addr      = "10.11.12.1" # where raft and blob listen; defaults to addr
# admin_port     = 9100         # /healthz and /readyz on bind_addr; absent runs neither
# data_dir       = "/var/lib/predastore"
# encryption_key = "/etc/predastore/master.key"
# tls_cert       = "/etc/predastore/server.pem"
# tls_key        = "/etc/predastore/server.key"
# tls_ca         = "/etc/predastore/ca.pem"     # formation CA; needed by endpoint_port

  [[host.node]]
  id   = 1
  role = "gate"                 # the S3 endpoint; port is the S3 port
  port = 8443
  # bind_addr = "0.0.0.0"       # where S3 listens; gate only, defaults to the host's
  # endpoint_port = 8474        # VPC endpoint listener; gate only, absent runs none

  [[host.node]]
  id   = 2
  role = "meta"
  port = 6660

  [[host.node]]
  id   = 3
  role = "blob"
  port = 9991
  # data_dir = "/mnt/disk1"     # per-node, so blob nodes can sit on separate disks

[[auth]]
access_key_id     = "AKIAIOSFODNN7EXAMPLE"
secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
account_id        = "123456789012"

# [[bucket]]
# name       = "my-bucket"
# region     = "ap-southeast-2"
# account_id = "123456789012"
# public     = false

# [iam]
# nats_url          = "nats://127.0.0.1:4222"
# master_key_path   = "/etc/spinifex/master.key"
# access_keys_bucket = "iam_access_keys"

# [ratelimit]
# enabled = true
# rate    = 100
# burst   = 200
```

## Addresses

Host addresses carry no port; a node's port comes from its own entry. `addr` is the literal address a peer observes, and is also what an accepted connection is matched against when the pool decides whether to keep it — so it may not be a wildcard or a multicast address. A hostname or a NAT between hosts resolves to nothing there, and such a connection is simply not pooled.

## Two planes

The S3 API is a public service and replication is not, so they bind separately:

| Plane | Carries | Address |
|---|---|---|
| Cluster | Raft and blob traffic, over QUIC | The host's `bind_addr`, defaulting to `addr` |
| Public | The S3 API, over HTTPS | The gate's own `bind_addr`, defaulting to the host's |

On a multi-homed machine the host binds a private address and the gate binds `0.0.0.0`, so S3 answers on every interface while consensus and shard traffic stay on the interface peers actually use. Both may be a wildcard; neither is required, and a host naming only `addr` binds that one address for both. Only a gate may name one — the other roles have no listener of its own to point anywhere, and `Validate` rejects it.

The defaults are the safe direction: a host that says nothing puts the cluster plane on `addr`, which is by construction the address peers dial and nothing wider.

## The admin listener

`admin_port` puts `/healthz` and `/readyz` on the cluster plane. Health is operator traffic and the S3 port is public, so it never shares one; the port is validated against the host's node ports the same way theirs are against each other, which is what stops it landing on the S3 port. It is a property of the process rather than a role, so it is written on `[[host]]` rather than becoming a fourth node role, and a host that names none runs no listener at all.

`/healthz` is liveness: reaching the handler is the whole check, and a process whose cluster is broken still answers 200. Restarting it would not help.

`/readyz` is what this process can serve, assembled from the roles the host runs. A meta replica contributes whether it observes a leader; a gate contributes a meta read and a probe of every blob node, and fails when fewer than `data` of them answer — below that a read cannot be reconstructed at all. Both probes ask for a key that does not exist, because a missing key is an answer only a working node can give. The response names each check and whether it passed, and nothing else: the port is unauthenticated, and a check's error carries addresses and object names.

None of that runs on the request path. A background sampler re-runs every check on a fixed interval and stores the outcome; `/readyz` only ever reads the last stored result, so its cost does not scale with the checks it answers for or with how often it is asked. This bounds the answer's staleness to one interval rather than bounding it to zero at the cost of letting an unauthenticated caller drive unbounded probe traffic against the meta plane and every blob node. A request that lands before the first cycle completes gets 503 rather than a guess.

## What validation rejects

Version mismatch, missing region, an erasure code that is absent or invalid, `parity = 0` at any width but 1, `data + parity` exceeding the blob node count, duplicate host or node ids, an unknown role, a missing or duplicated port within a host, an `admin_port` that is out of range or already a node's, an `endpoint_port` off a gate, out of range, already a port on its host, or without the host's `tls_ca`, a port on a host or node address, a wildcard `addr`, a relative path anywhere, a `data_dir` on a gate, a `bind_addr` on anything but a gate, two gates on one host, two nodes deriving the same data directory, and two nodes at the same address. A malformed bucket name is dropped with a warning rather than being fatal, since the rest of the config is still serviceable.

## Dev profiles

`config/` ships three ready-made layouts driven by `scripts/start.sh`. `1host` is one process over the pipe at RS(1,0); `3host` and `5host` put every host on one machine behind loopback aliases at RS(2,1) and RS(3,2), so they need `sudo` for the aliases and the trust anchor.

---

# 13. Failure handling

## Read path

| Failure | Detected as | Response |
|---|---|---|
| No index row for the shard | `not-found` from the blob node | The join fails; the gate refetches with parity and reconstructs |
| Fragment fails its GCM tag | `ErrIntegrity` | Same, and the node marks the shard for repair when its key is verified. Covers corruption, tampering, fragment swaps and a wrong master key indistinguishably |
| Node unreachable or slow | Stream error or deadline | Same |
| Fewer than `K` valid shards | Reconstruction fails | 500 to the client |
| Object metadata missing | Meta read fails | 404 `NoSuchKey` |

A shard written to disk but never committed is indistinguishable from one that was never attempted. Both are absent, and the commit invariant is what guarantees readers cannot observe a partial write.

## Write path

| Failure | Response |
|---|---|
| Any shard write fails | 500, or 507 `InsufficientStorage` when a node was full |
| Free space below the nearfull watermark | Write succeeds, `X-Predastore-Pool-Pressure: nearfull` returned, compaction kicked |
| Meta write cannot reach a leader | Retried across replicas, then 500 |
| Crash between shard commit and meta commit | The object does not exist; its shards are orphaned (see §15) |

## Cluster

A meta quorum loss stops all writes and leaves reads serving whatever the surviving replicas last applied. Losing up to `M` blob nodes leaves every object readable; losing more than `M` loses the objects placed on them. Nothing currently re-replicates a shard after a node is replaced.

---

# 14. Developer reference

## Package map

| Path | Purpose |
|---|---|
| `predastore.go`, `config.go` | The module's public surface: `Options`, `Run`, `Config`, `LoadConfig`, and the conversions from the file into each subsystem's settings |
| `cmd/s3d` | Flags, telemetry, signal context |
| `internal/config` | TOML parsing and validation; the `HostID`/`NodeID`/`Role` vocabulary |
| `internal/gate` | HTTPS listener, middleware chain, route table, credential chain |
| `internal/gate/handlers` | The S3 operations, one factory per operation |
| `internal/gate/model` | Stored record shapes, error taxonomy, name validation, object hashing, table names |
| `internal/gate/auth` | Config and NATS-KV credential providers, chained |
| `internal/gate/placement` | The consistent hash ring |
| `internal/gate/chunked` | `aws-chunked` transfer decoding |
| `internal/meta` | Raft replica, FSM, snapshots, rpc handlers, client |
| `internal/blob` | Shard rpc service and client |
| `internal/blob/engine` | Segments, fragments, extents, index, tombstones, compaction, encryption |
| `internal/rpc` | Resolver, connection pool, stream framing, mux and server |
| `internal/transport` | The pipe and QUIC transports behind one interface |
| `pkg/masterkey` | Master key loading, AEAD construction, fingerprints |
| `pkg/sigv4`, `pkg/iampolicy`, `pkg/auth` | Signature verification, policy evaluation, ARNs |
| `pkg/ratelimit`, `pkg/otelsetup` | Throttling and telemetry |

## Key files

| File | Why you would open it |
|---|---|
| `predastore.go` | How a host's nodes get built and started |
| `internal/config/config.go` | Every rule the config file is held to |
| `internal/rpc/resolver.go` | How a node id becomes an address |
| `internal/rpc/pool.go` | Connection reuse and the simultaneous-open tiebreak |
| `internal/gate/routes.go` | The S3 route table |
| `internal/gate/handlers/shards.go` | Erasure coding, shard fan-out, reconstruction |
| `internal/gate/handlers/meta.go` | The table-prefix convention over the flat key-value store |
| `internal/meta/streamlayer.go` | Raft over rpc streams |
| `internal/meta/fsm.go` | Command apply, snapshot format, restore |
| `internal/blob/engine/store.go` | Reservation, commit, delete, tombstones |
| `internal/blob/engine/fragment.go` | The on-disk fragment layout, seal and open |
| `internal/blob/engine/compaction.go` | Candidate selection and relocation |
| `internal/blob/engine/state.go` | The crash-safe counter and `storeID` handling |

## Tunables

| Constant | Value | Where |
|---|---|---|
| `fragBodySize` | 8 KiB | `engine/fragment.go` |
| `totalFragSize` | 8240 B | header 32 + body 8192 + tag 16 |
| `bufLen` | 32 fragments | Per-stream read/write window, ≈ 256 KiB |
| `DefaultMaxSegSize` | 4 GiB | Segment roll threshold |
| `fragNumReservation` | 1 048 576 | fragNums per durable reservation |
| `defaultCompactionInterval` | 5 min | Overridden by `[compaction] interval_seconds` |
| `compactionLiveThreshold` | 0.70 | Live-byte fraction below which a segment is a candidate |
| `DefaultCompactionMaxBytesPerSec` | 64 MiB/s | Overridden by `[compaction] max_bytes_per_sec` |
| `DefaultCompactionMaxYield` | 60 s | Overridden by `[compaction] max_yield_seconds` |
| `defaultNearfullFreeFrac` | 0.15 | Kicks compaction, flags pressure |
| `defaultFullFreeFrac` | 0.05 | Rejects writes with `ErrStoreFull` |
| `statfsThrottleInterval` | 1s | Free-space check throttle |
| `statfsBytesInterval` | 64 MiB | Forces a check regardless of the throttle |
| `maxSegmentScanAttempts` | 100 | Bound on the walk for a non-full segment |
| `maxHeaderSize` | 1 MiB | rpc stream header cap |
| QUIC streams | 1000 | Identical on the dial and listen sides |
| Ring partitions / replication / load | 5 / 100 / 1.25 | `gate/placement` |
| `maxParallelPartFetches` | 10 | Multipart completion fan-out |

## Working on it

```bash
make build            # GOFIPS140=v1.0.0 go build -o ./bin/s3d ./cmd/s3d
make certs            # dev TLS certs the integration tests serve
make test             # unit tests
make preflight        # what CI runs: lint, govulncheck, coverage, integration
make fix              # auto-fix what the linter can
./scripts/start.sh -w 1host   # a running cluster on https://127.0.0.1:8443
./scripts/stop.sh ; ./scripts/clean.sh
./scripts/bench.sh 3host      # or `disk` for the engine alone
```

`make preflight` must pass before committing.

---

# 15. Known gaps

Things the design implies but the code does not do. Longer-horizon items live in [TODO.md](./TODO.md).

**No repair.** Nothing reconstructs a shard that a node lost or never received. A GET rebuilds the object on the fly from parity, but does not write the missing shard back, and there is no background healer reconciling a node's contents against the ring. Replacing a blob node therefore leaves every object it held permanently down one shard until it is rewritten.

**No index rebuild.** If a blob node's badger index is lost, its data is unreachable through that node. The `.idx` sidecars do record the key of every extent ever allocated, so a rebuild is mechanically possible — but no such path is implemented, and the extents would still need validating against the index that decides which of several allocations for a key is live.

**Orphaned shards after a crash.** Shards commit before metadata. A crash in between leaves shards that nothing references and nothing tombstones, so compaction never reclaims them. A startup scrubber reconciling a node's index against the metadata plane would close this.

**No rebalancing.** Adding a blob node changes the ring for new objects only. Existing objects stay where they were placed, and nothing migrates them. Changing the erasure code likewise applies to new objects only, and old objects are read at the width recorded in their placement.

**Two Raft round-trips per PUT.** The placement and the listing key are written separately. A batched apply would halve the metadata cost of a write.

**No ETag backfill for pre-v3 objects.** `PutObject` and `CompleteMultipartUpload` store the body's MD5 in placement record v3 — a plain hex digest for a single part, `"<md5>-<N>"` for a multipart object assembled from N parts — and `GetObject`, `HeadObject` and `ListObjectsV2` return it as the ETag. A record written under an older placement version has no digest, so these surfaces omit its ETag instead of serving a stale value, and nothing rewrites it to add one.

**A delimiter listing reads the whole prefix.** A `ListObjects` page with a delimiter scans every row under the prefix and collapses them before it cuts the page, so its cost grows with the prefix. A page without a delimiter stops on the `max-keys` + 1st live row, and the replica copies no value past it. Badger still reads ahead one live item, walks the tombstones before it and copies inline values of what it reads ahead, and its value prefetch still runs. It still pays for keys deleted since badger last compacted, which it merges across its levels; that cost can scale with the number of recent deletes.

**No multi-object delete.** `POST /{bucket}?delete=` has no route and returns 405.

**No key rotation.** One cluster-wide master key, no envelope layer, no per-bucket or per-tenant keys, and no migration path for data written under a different key.

**Whole objects are buffered in memory.** Both the PUT and GET paths hold the full object, and multipart completion additionally stages the assembled object through a temp file.
