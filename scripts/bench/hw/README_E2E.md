# Predastore on real hardware

`hwbench.sh` runs a three-host predastore cluster on bare metal — bottlebrush,
ironbark and casuarina — and measures it. It is the bare-metal counterpart to
`make e2e-stress`, which runs four hosts as loopback aliases on one machine.

The point is the wire. Locally, "inter-node traffic" is a memcpy through the
loopback interface. Here it is QUIC over a 25 GbE link between separate
machines, with real NICs, real NVMe and real scheduling. A cost that is
invisible on loopback is measurable here, which is the whole reason the profile
exists.

## It has nothing to do with spinifex

These hosts run a spinifex-managed predastore as `spinifex-predastore`, under
`/var/lib/spinifex/predastore`, deployed by `scripts/update-nodes.sh` in the
`mulga` repo. **This is not that.** `hwbench.sh` deploys predastore *alone*,
the same way `make e2e-stress` runs predastore alone locally — no spinifex, no
NATS, no daemon, no viperblock.

The two coexist because `hwbench.sh` is deliberately built to touch nothing the
system owns:

| Concern | How it stays out of the way |
|---|---|
| **Root** | Never used. Nothing runs as root and nothing is installed |
| **Trust store** | `SSL_CERT_FILE` points Go's x509 at the cluster's own cert, so nothing is written to `/usr/local/share/ca-certificates`. This is the single trick that keeps it sudo-free |
| **Binary** | Static (`CGO_ENABLED=0`), pushed by `scp`, run from the deployment dir |
| **Service manager** | None. `setsid --fork`, not systemd |
| **Filesystem** | Everything under `$HW_ROOT`, one deletable directory |
| **Ports** | Gate on **8333**, not 8443 — see below |
| **Process name** | `s3d`. The spinifex services are all `spx`, so they cannot be confused |

### The gate is on 8333, and this is not optional

`spinifex-predastore` binds `*:8443` — a wildcard, so it already owns every
address on this machine at that port. A second bind fails with `EADDRINUSE` and
the gate never comes up.

The profile therefore uses **8333**. Meta (6660) and blob (9991) keep their
defaults; nothing else on these boxes listens there. If you move this to other
hardware, check before assuming:

```bash
ssh tf-user@bottlebrush 'sudo ss -lntp | grep -E ":(8333|6660|9991)\b"'
```

## Safety on these three machines

They are the hypervisors for the `env1`–`env21` nested dev clusters **and for
the workstation VM you are typing in**. Read `CLAUDE.local.md` before you
improvise.

- **Never `pkill` QEMU by name here.** It takes down the dev environments and
  your own session. `hwbench.sh stop` uses `pkill -x s3d`, which is exact-match
  on a name nothing else uses — that is safe, and it is why it is written that
  way.
- **`$HW_ROOT` is on `/mnt/disk1`, which also holds `envN` guest disks.** A
  predastore that fills it takes those dev environments with it. Check before a
  large run, and clean up after one:

  ```bash
  scripts/bench/hw/hwbench.sh status
  ssh tf-user@bottlebrush 'df -h /mnt/disk1; du -sh /mnt/disk1/tf-user/predastore'
  ```

  As of 2026-09-01 each host was carrying **~165 GB** of leftovers from earlier
  runs — under the old `/mnt/disk3` root, so `hwbench.sh clean` no longer sees
  them. They have to be removed by path:

  ```bash
  for h in bottlebrush ironbark casuarina; do
      ssh tf-user@$h 'du -sh /mnt/disk3/tf-user/predastore'   # look first
  done
  ```
- The host firewall is off on these machines and stays off, so no rules are
  needed. Do not turn it on to "help".

### Which drive, and why it changes the numbers

These hosts have three separate NVMe drives plus a slower root volume:

| Mount | Device | Carries |
|---|---|---|
| `/mnt/disk1` | `nvme0n1` | `envN` guest disks — **and this harness** |
| `/mnt/disk2` | `nvme1n1` | `/var/lib/spinifex` (bind mount) |
| `/mnt/disk3` | `nvme2n1` | spinifex's predastore, plus `envN` guest disks |

`HW_ROOT` defaults to **disk1** because it is the only one with no storage
service writing to it. Putting this harness on disk2 or disk3 puts it on the
same spindle as spinifex's own predastore or viperblock, and then every number
it produces is confounded by their I/O.

Never use the root volume. It is a slower drive, and a benchmark on it measures
the drive rather than the change.

disk1 is not empty — it holds `envN` guest disks — so it is a choice about
*contention from a storage service*, not about having the drive to yourself.

## Commands

```bash
cd predastore
scripts/bench/hw/hwbench.sh build          # static s3d per ref, into $HW_WORK
scripts/bench/hw/hwbench.sh deploy <ref>   # push the binary, cert, key, config
scripts/bench/hw/hwbench.sh tools          # warp to all three, AWS CLI to one
scripts/bench/hw/hwbench.sh start  <ref>   # launch all three, wait for a leader
scripts/bench/hw/hwbench.sh verify <ref>   # round trip 8 MiB through every gate
scripts/bench/hw/hwbench.sh perf   <ref> [tag]
scripts/bench/hw/hwbench.sh status
scripts/bench/hw/hwbench.sh stop
scripts/bench/hw/hwbench.sh clean          # stop, then remove $HW_ROOT everywhere
```

A first run is `build → deploy → tools → start → verify`. Run with no arguments
to get the header as usage.

### Environment

| Variable | Default | Purpose |
|---|---|---|
| `HW_HOSTS` | `tf-user@bottlebrush tf-user@ironbark tf-user@casuarina` | ssh targets, **in host-id order** |
| `HW_ADDRS` | `10.10.8.4 10.10.8.5 10.10.8.6` | Their br-lan (25 GbE) addresses, same order |
| `HW_ROOT` | `/mnt/disk1/tf-user/predastore` | Deployment root. See "Which drive" below |
| `HW_REFS` | `dev:origin/dev base:… tip:…` | `name:committish` pairs to build |
| `HW_WORK` | `/tmp/hwbench` | Local build dir. **A reboot clears `/tmp`** |
| `HW_SSH_TIMEOUT` | 120 | Bounds every remote command |
| `HW_TOOLS_HOST` | last host | Where the AWS CLI and the harness run |
| `PERF_PRESET` | `compare` | `smoke` for 30 s samples, `compare` for two-minute ones |

`HW_ADDRS` must be the br-lan addresses, not the management ones. Getting this
wrong is the failure that looks like a working cluster with inexplicably poor
numbers, because the overlay is then crossing the 1 GbE management path.

## Why `verify` is not a formality

A cluster that is listening is not a cluster that works. `verify` PUTs 8 MiB
through the first gate and reads it back through **every** gate, comparing
SHA-256. At RS(2,1) over three hosts each shard lands on exactly one host, so
the two gates that did not take the write must fetch a shard from a peer. That
makes the readback the proof that QUIC is actually carrying data, not just that
three processes are up.

## What `perf` does, and why it is not a second harness

`perf` does not reimplement the benchmark. It ships `scripts/lib.sh` and
`scripts/bench/e2e-performance.sh` to the tools host and runs the **same
harness** in its external-hosts mode (`PERF_EXTERNAL_HOSTS`), against the
cluster `start` already launched. Same workloads, same preset sizing, same
analysis — so a bare-metal number is directly comparable with the loopback one
it replaces.

It runs on a host rather than on your workstation because the workstation has no
route to the cluster network.

Server logs are saved per run by `save_host_logs`, because `start` truncates
them on every launch and the external harness has no route to them. Without
that, the only account of a failed run is the client's side of it.

## Running the stress gate here — not wired up yet

`hwbench.sh` has `perf` but **no `e2e-stress`**. Wiring one up is not the same
size of job as `perf` was, and it is worth understanding why before you try.

`e2e-performance.sh` only needs endpoints, so external mode was a small change.
`e2e-stress.sh` *owns* its cluster:

- It calls `start.sh` and `stop.sh` repeatedly — the `repair`, `handoff`,
  `node-rejoin`, `node-resync` and `node-rebuild` scenarios each stand up their
  **own purpose-built cluster** with its own profile and its own retention
  settings.
- It injects faults with `kill -STOP` on local pids read from
  `$PID_DIR/host-N.pid` — 34 signal calls across 14 pid lookups.
- `node-rebuild` wipes a node's data directory directly.

So there are two honest options, and they buy different things:

| Approach | Gets you | Cost |
|---|---|---|
| Run `e2e-stress.sh` unchanged **on one host** | Real NVMe, real CPU, 256 threads. Still loopback for inter-node traffic | Almost none — but needs sudo on that host for loopback aliases and the trust store, which is the one thing `hwbench.sh` currently avoids |
| Teach `e2e-stress.sh` an external mode | The real thing: faults across a real network | An indirection over start/stop/signal that is local by default and ssh-based when configured, plus deciding what the five self-provisioning scenarios do against a fixed cluster |

The second is the one worth having. It needs a plan doc and a bead before code,
and the open design question is what `repair`, `handoff` and the three `node-*`
scenarios do when they cannot provision their own cluster — the likely answer is
that a hardware run covers the scenarios that use the main cluster
(`concurrent-put`, `torn-overwrite`, `stale-shard`, `large-object`,
`multipart-upload`, `last-modified`, `freeze`) and the rest stay local.

Do not solve this by copying `e2e-stress.sh` and editing it. Two stress harnesses
that drift apart is a worse outcome than not having the hardware one.

## Gotchas

- **`HW_WORK` defaults into `/tmp`**, which a reboot clears — along with the
  build worktrees and the generated cert and master key. Losing the key means a
  deployed store cannot be read; losing the cert means the peers no longer trust
  each other. Pin `HW_WORK` for anything long-lived.
- **`start` wipes the data dir on every launch.** That is deliberate, so one ref
  is never measured against a fuller disk than another — but it means `start` is
  not a resume.
- **`stop` confirms rather than assumes.** It waits up to 45 s, then `SIGKILL`s,
  then reports the surviving count. A stop that returned early would serve the
  next ref's measurement from the previous ref's binary.
- **`GOFIPS140=v1.0.0` is required at build time**, not optional — `fipsboot`
  panics at init without it, and the failure surfaces as a process that dies at
  startup on the far host rather than as a build error you can see.
- **`GOWORK=off` at build time** so each ref resolves its own declared
  bluebottle rather than whatever the workspace has checked out.
- **The AWS CLI is shipped to one host only**, as a 245 MB tree. It addresses
  any gate over the network, so correctness does not need it everywhere.
