# Running the end-to-end stress gate

`make e2e-stress` stands up a four-host predastore cluster, injects faults into
it, and asserts what survives. It is the gate that catches the class of bug unit
tests structurally cannot: anything that needs a real cluster, a real erasure
stripe across separate processes, and a node that stops answering mid-write.

This document is for someone who has not run it before, and for setting it up on
a CI runner.

## What it is not

**It does not deploy anything.** The cluster it tests is four `s3d` processes on
the machine you run it on, bound to loopback aliases `10.11.12.1`–`.4`. It never
connects to bottlebrush, ironbark, casuarina, or any other host, and it holds no
credentials for one.

Deploying predastore to the dev-prod hosts is a separate operation with a
separate tool — `scripts/update-nodes.sh` in the `mulga` repo, which builds all
five sub-repos from your **working tree** and installs them. The two are
unrelated, and passing this gate does not put anything on a server.

## What a default run does

Eleven scenarios in order, then a Warp load test. A default run is about **25
minutes** and asserts **156** things.

| Scenario | Asserts | What it does |
|---|---|---|
| `repair` | 4 | Writes with a host down, thaws it, asserts the sweep restores its shards and the restored parity is sound |
| `handoff` | 4 | Writes at full stripe width with a host down, asserts the shard lands one step along the ring and comes home |
| `node-rejoin` | 7 | Takes a node away and returns it within the retained raft log |
| `node-resync` | 15 | Same, but the profile pins retention so the node must resync rather than catch up |
| `node-rebuild` | 23 | Same, but the node's disk does not survive |
| `multipart-upload` | 6 | Large multipart PUT and readback |
| `last-modified` | 10 | Timestamp semantics across overwrite |
| `large-object` | 10 | 2 GiB and 4 GiB objects, with an RSS sampler — a size with no disk is **skipped loudly** |
| `concurrent-put` | 24 | 6 writers race one key, 8 keys × 3 rounds, on a healthy cluster |
| `torn-overwrite` | 5 | Overwrites with the host holding one named shard stopped; asserts the failed write left the object exactly as it was |
| `stale-shard` | 48 | Overwrites with a host frozen, asserts no read ever splices generations |
| `freeze` | 6 | Warp GET load while a host is SIGSTOPped; survivors keep serving, frozen host rejoins raft and takes writes |

`partial-put` — a client that stops sending mid-body — exists but is **not** in a
default run. Name it explicitly.

**SIGSTOP is the fault worth injecting** because it is the one a healthy
transport cannot distinguish from a slow peer: the process stays dialable and
its sockets stay open while it answers nothing. That is precisely the state a
connection pool can sit on indefinitely.

## Requirements

### Commands

Checked at startup, and the run exits rather than half-working without them:

```
aws  awk  curl  diff  git  go  openssl
```

Plus Go **1.27.0** (from `go.mod`) and `warp`, which `make warp-install` fetches
into `bin/tools/warp` if it is missing.

`shellcheck` is a `make preflight` dependency, not an e2e one, but you want it on
the same runner.

### Root, and why

This is the part that decides where you can run it. `scripts/start.sh` needs
`sudo` twice, and neither is avoidable:

1. **Loopback aliases.** `sudo ip addr add 10.11.12.N/24 dev lo` for each host.
   Four processes cannot share a port on one address, so the profile gives each
   host its own. Needs `CAP_NET_ADMIN`.

2. **The OS trust store.** `s3d` verifies QUIC peer certificates strictly
   against the OS trust store — `transport.NewQUICTransport` is built with no
   `RootCAs` override — so the self-signed cert is copied to
   `/usr/local/share/ca-certificates/predastore-<cluster>.crt` and
   `update-ca-certificates` is run. Without it, peers fail with `certificate
   signed by unknown authority` and **the cluster never elects a leader**. A
   single-host profile opens no QUIC socket and needs none of this.

The run must be able to `sudo` without a password prompt.

### Disk

| Path | Holds | Notes |
|---|---|---|
| `$HOME/.cache/predastore-e2e` | The cluster work dir | Used for `all`, `large-object`, `multipart-upload` |
| `$TMPDIR` | Same, for other scenarios | |
| `scripts/bench/results/e2e-stress/` | Results, ~5 MB per run | Kept; nothing prunes it |

The work dir is **deliberately not `$TMPDIR`** for the big scenarios. `TMPDIR`
on a developer box is commonly tmpfs, which is RAM-backed — that would both cap
the object sizes and charge the object's own bytes to the memory figure the
scenario exists to measure. Override with `STRESS_WORK_ROOT`.

Budget **~30 GB free**. The large-object scenario writes 2 GiB and 4 GiB objects
sharded across four hosts with parity. A size with no room is skipped and
reported as `skipped-no-disk` — it does not fail the run, so a runner that is
quietly too small **passes with less coverage than you think**. Check the
`large_object=pass (N size(s) skipped for disk)` line in `run-info.txt`.

### Ports

Every node port is shifted by `STRESS_PORT_OFFSET`, default **10000**, so a run
does not collide with a cluster already on the defaults. For `4host`:

| Role | Config | In a run |
|---|---|---|
| gate (S3) | 8443 | 18443 |
| meta (raft) | 6660 | 16660 |
| blob | 9991 | 19991 |

Four hosts × three roles = 12 listeners, on `10.11.12.1`–`.4`.

## Running it

```bash
make e2e-stress                                  # the full gate
STRESS_SCENARIO=concurrent-put make e2e-stress   # one scenario
```

`make e2e-stress` depends on `build certs warp-install`, so a clean checkout
needs no preparation.

Useful knobs — the full list is the header of `scripts/bench/e2e-stress.sh`:

| Variable | Default | Purpose |
|---|---|---|
| `STRESS_SCENARIO` | all | One scenario. **Validated, not defaulted** — a typo exits rather than quietly running everything and reporting a pass for a test that never ran |
| `STRESS_CONFIG` | `4host` | Profile from `config/` |
| `STRESS_HOST` | `follower` | Which host gets frozen; `leader` or an explicit id. Resolved against the running cluster, since which host raft elects varies per run |
| `STRESS_FREEZE` | 90 | Seconds the host stays frozen |
| `STRESS_KEEP_WORK` | 0 | `1` keeps the work dir for post-mortem |
| `STRESS_PORT_OFFSET` | 10000 | |
| `STRESS_LARGE_SIZES` | `2GiB 4GiB` | The header comment says `2GiB 4GiB 8GiB`; the code says two sizes. The code is right |
| `STRESS_WORK_ROOT` | see above | |

## Reading the result

Exit status is the verdict — non-zero if any scenario failed. Verdicts are held
to the end deliberately, so a red scenario early does not cost you the coverage
of the ones after it.

Results land in `scripts/bench/results/e2e-stress/<UTC-stamp>-<short-sha>/`:

| File | Contents |
|---|---|
| `run-info.txt` | The one to read. Config, versions, an `Assertions` block, and the full timeline |
| `events.txt` | Timeline alone |
| `logs/` | Every node's log |
| `concurrent-put/` | Per-key `.race` files — what each writer was told |
| `get-latency.txt` | Warp throughput and percentiles |
| `*.json` | Objects preserved from a failing assertion |

The `Assertions` block is the summary:

```
repair=pass
handoff=pass
torn_overwrite=pass
concurrent_put=pass
stale_shard=pass
large_object=pass (0 size(s) skipped for disk)
```

The run cleans up after itself on `EXIT INT TERM` — it continues any frozen
process first, because a stopped process never handles SIGTERM and would
otherwise be left holding its ports and failing the *next* run somewhere
unrelated. If a run is killed hard, `scripts/clean.sh` stops everything and
removes cluster data.

## Setting it up on a GitHub runner

### Use a self-hosted runner

The existing `.github/workflows/predastore.yml` runs unit tests, race detection
and lint on `ubuntu-26.04` hosted runners. The e2e gate does not belong there:

- **Disk.** Hosted runners give roughly 14 GB free. The large-object scenario
  wants more, and it *skips rather than fails*, so a hosted run would report a
  pass with reduced coverage.
- **Time.** ~25 minutes per run, against a hosted-runner budget shared with
  everything else.
- **Trust store.** The run writes to `/usr/local/share/ca-certificates` and runs
  `update-ca-certificates`. Fine on an ephemeral VM, but it is a system
  modification that has to be understood, not inherited.

A hosted runner *can* run a subset — `concurrent-put`, `torn-overwrite`,
`repair`, `handoff` all fit comfortably. That is a reasonable PR gate with the
full run nightly on a self-hosted box.

### Runner prerequisites

```bash
sudo apt-get install -y awscli curl git openssl shellcheck
# Go 1.27.0 — match go.mod; actions/setup-go with go-version-file works too
```

The runner user needs passwordless sudo for `ip` and `update-ca-certificates`.
Scope it rather than granting blanket sudo:

```
# /etc/sudoers.d/predastore-e2e
runner ALL=(root) NOPASSWD: /usr/sbin/ip addr add * dev lo, \
                            /usr/sbin/ip addr del * dev lo, \
                            /usr/bin/update-ca-certificates, \
                            /bin/cp * /usr/local/share/ca-certificates/*
```

**A self-hosted runner is not ephemeral.** Three consequences:

- Loopback aliases and the trust anchor persist between runs. That is harmless
  and makes later runs slightly faster, but it means a runner that has run this
  once is no longer a clean machine.
- Results accumulate in `scripts/bench/results/e2e-stress/`. Nothing prunes
  them.
- A hard-killed run can leave `s3d` processes holding ports. Start each job with
  `scripts/clean.sh`.

### Workflow

```yaml
  e2e_stress:
    name: E2E Stress
    runs-on: [self-hosted, linux, x64, predastore-e2e]
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-go@v7
        with:
          go-version-file: go.mod
          cache-dependency-path: go.sum

      # A previous hard-killed run can leave s3d holding ports, which fails
      # this run somewhere unrelated to the change under test.
      - name: Clear any leftover cluster
        run: scripts/clean.sh || true

      - name: Run the gate
        run: make e2e-stress

      - name: Publish results
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: e2e-stress-${{ github.run_id }}
          path: scripts/bench/results/e2e-stress/
          retention-days: 30
```

`if: always()` on the upload matters more than it looks: a failing run's
`run-info.txt` and node logs are the only explanation of *why*, and they are
exactly the runs whose artifacts you need.

### Do not gate on the throughput numbers yet

`get-latency.txt` carries Warp throughput, and it is tempting to fail a build on
it. Two runs of this harness on an otherwise-idle workstation came in at 504 and
486 MiB/s — within 3.5% on the average, but with p50 at 132 ms and 196 ms. On a
shared runner that spread will be wider. Bead `mulga-wpjsx` covers doing this
properly with a real baseline.

## Invariant 11: the script is frozen

`scripts/bench/e2e-stress.sh` is pinned by content hash so that a change to the
measuring instrument is never mistaken for a change in what it measures:

```bash
git hash-object scripts/bench/e2e-stress.sh
# 3c5e10daf1fcffaab1c9ce76c03108f0c6fe8837
```

If you change the harness, the freeze moves — and the change has to be proven
neutral against unchanged Go before any behaviour change lands beside it. Record
the new hash in the plan doc that changed it. Previous baselines:
`89f1374e...`, `8b6c8459...`, `6a91ebdc...`.

## Gotchas

- **A single green run is not evidence** for anything concurrency-related. The
  candidate fix for the concurrent-PUT bug passed twice and was then shown by
  the full gate to still lose objects. Run a racing scenario five times.
- **`predastore/scripts/stop.sh` now confirms termination**, and fails rather
  than reporting a stop that did not happen. It waits `STOP_TIMEOUT` (20s) for
  SIGTERM, escalates to SIGKILL, waits `KILL_TIMEOUT` (5s) more, and exits 1
  naming anything that survived. A pidfile is removed once its process is gone
  rather than once it has been signalled. Before this it deleted the pidfiles
  regardless, so an `s3d` that ignored the signal kept its ports and broke the
  next run looking like a fault in whatever was just built.
- **`stop.sh` takes cluster names, and used to ignore them.** `stop.sh 3host`
  now stops that cluster alone; with no names it sweeps every cluster under
  `$PREDA_DIR`, which is what `clean.sh` and the benchmark harnesses rely on.
  Naming a cluster that is not there warns and exits 0, because CI runs its
  teardown step unconditionally and "already stopped" is a legitimate outcome.
  `-w` is accepted and ignored for symmetry with `start.sh`. Before this the
  arguments were silently discarded and every call stopped everything.
- **A full disk breaks the linter with an unrelated message** —
  `no go files to analyze: running go mod tidy may solve the problem`. Check
  `df -h` first; `go clean -cache` reclaims several GB.
- **The header comment's scenario list is stale.** It predates `concurrent-put`,
  `node-rejoin`, `node-resync`, `node-rebuild` and `multipart-upload`. The
  authoritative list is the `case` statement that validates
  `STRESS_SCENARIO`.
- **A SIGSTOPped host keeps connections that died under it**, and the pool only
  evicts one after three stalls of five seconds each. A scenario that freezes a
  host and a later one that writes through it will see that delay as a cluster
  fault. `warm_gate` exists for this; call it after any thaw.
