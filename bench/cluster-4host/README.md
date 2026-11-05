# A whole reference cluster, three members on three hosts

`run-cluster-4host.sh` drives the reference's own cluster orchestration
(`scripts/aeron/remote-cluster-benchmarks` from the harness checkout) with three
members on three machines and the client, with its own driver, on a fourth.
Every node is the reference's — consensus module, archive, clustered service —
and the only thing that changes between the two scenarios of a round is the
**media driver** under it:

| arm | media driver |
|---|---|
| `java` | the reference's Java driver |
| `deepmsg` | this build's driver, through the harness's `deepmsg-media-driver` |

A reading here says what this build's driver is worth when a whole reference
cluster runs on top of it. It says **nothing** about this build's consensus
module, archive or container: none of them is in the cell. Those are
`bench/cluster-arms/`, which runs four arms (`j` / `j-d` / `r-a` / `r`) over
three members on three hosts with the client on a fourth.

## What it needs

- **Four machines of one type**, on one VPC/subnet. The 2026-11-02 run used
  `ecs.c7.8xlarge` (16 cores, no SMT, 64 GiB). The four addresses in
  `hosts.env` are **examples**: a run that still has one of them and was not
  given a machine in the environment is refused, by name, rather than allowed
  to measure a host nobody described.
- **Key-based ssh from the controller (node 0) to all four machines, including
  itself.** The harness generates a script per member and ships it over ssh, and
  node 0 is the controller *and* member 0, so `ssh -o BatchMode=yes root@node0
  true` has to work too. `CLUSTER_SSH_KEY` names the private key
  (`$HOME/.ssh/id_ed25519`); its public half must be in root's
  `authorized_keys` on every machine. That first trust is out of band — the
  cloud console, or one password login and `ssh-copy-id` — because
  `provision-host.sh` itself travels over the same ssh.
- **`provision-host.sh --check <user@host>`**, then again without `--check`, on
  every machine that is not the controller. It copies the harness, this build's
  driver and an offline JDK (with the `/etc` trees the JDK's absolute symlinks
  point at), and sets `rmem_max` — the Java driver only warns about the image's
  ~416 KiB, the C driver exits.
- **The harness patched to know this build's driver**:
  `bench/cluster-arms/patches/install.sh --apply`, which adds
  `scripts/aeron/deepmsg-media-driver`, the `drivers['deepmsg']` row in
  `remote-cluster-benchmarks`, and the two name tables the harness's wait and
  kill paths read.
- **The receive interrupts on cores that do not sleep**, on all four:
  `bench/tools/irq-steer.sh apply`. Without it a cluster cell measures C-state
  wakeups; see `bench/README.md`, "The interrupt layout".

## Setting the machines up, in order

Each step below is checked by the next one, and the order is the one that fails
readably: ssh before provisioning, provisioning before the plan, the plan before
a cell.

1. **Four instances, one type, one VPC/subnet.** The 2026-11-02 run used
   `ecs.c7.8xlarge` (16 cores, no SMT, 64 GiB). Note the four private
   addresses; they are the only addresses the batch uses.

2. **Name them.** Edit `hosts.env`, or export the four variables (the examples
   in that file are refused until you do):

   ```
   export CLUSTER_NODE0_SSH=root@<node0> CLUSTER_NODE1_SSH=root@<node1>
   export CLUSTER_NODE2_SSH=root@<node2> CLUSTER_CLIENT_SSH=root@<client>
   ```

3. **One key that reaches all four, itself included.**

   ```
   key=${CLUSTER_SSH_KEY:-$HOME/.ssh/id_ed25519}
   ssh-keygen -t ed25519 -f "$key"                    # if there is none yet
   for h in <node0> <node1> <node2> <client>; do
       ssh-copy-id -i "$key.pub" root@$h
   done
   ssh -i "$key" -o BatchMode=yes "root@<node0>" true  # the controller reaches itself
   ```

   `CLUSTER_SSH_KEY` names the key the batch uses; when that file does not
   exist, the scripts fall back to whatever `ssh` would pick (an agent, the
   default identity). The first `ssh-copy-id` per machine needs a password or
   the console; there is no way from here to make that first trust. Everything
   after it is `BatchMode=yes`.

4. **On the controller: the two checkouts, this build, the harness patch.**
   `bench/README.md`, "Getting the two checkouts", then `bench/build.sh`, then
   `bench/cluster-arms/patches/install.sh --apply`. The harness and the driver
   must sit at `$CLUSTER_BENCHMARKS_PATH` and `$CLUSTER_DEEPMSG_DIR` — the same
   paths on all four, because those paths go into the scripts the harness
   generates. The controller is member 0 as well, so it needs the same JDK and
   the same `rmem_max` as the other three: it is not exempt from the list above.

5. **Provision the other three.**

   ```
   for h in <node1> <node2> <client>; do
       bench/cluster-4host/provision-host.sh --check "root@$h"   # what is missing
       bench/cluster-4host/provision-host.sh "root@$h"           # put it there
   done
   ```

   It is idempotent and it is the only step that writes to another machine.

6. **The interrupt layout, on all four.**

   ```
   bench/tools/irq-steer.sh apply     # and the same command on the other three
   ```

7. **Ask the batch what it sees.**

   ```
   bench/cluster-4host/run-cluster-4host.sh --list
   ```

   The preflight checks ssh to all four, the four files a member needs, java,
   and that the driver digest is one digest. It exits non-zero when the plan is
   not runnable; fix what it names before going on.

8. **Smoke, then the batch.** `--quick` is 5 s per arm and asks only whether a
   cell forms with both drivers; the full run is four tiers, two rounds,
   reversed.

9. **Put the machines back.** `bench/tools/irq-steer.sh restore` on all four,
   and leave no process behind: `pgrep -f '[i]o\.aeron'` is empty, and the
   members' `cluster-data/{cluster,archive}` are removed before the next batch.
   A steered machine must not be left steered.

## Running it

```
bench/cluster-4host/run-cluster-4host.sh --list      # the plan, paths and preflight; runs nothing
bench/cluster-4host/run-cluster-4host.sh --quick     # 5 s per arm, plumbing only
bench/cluster-4host/run-cluster-4host.sh             # four tiers, 60 s + 30 s warmup, two rounds
```

`--rounds 1,2` and `--fsync 0,2` narrow the grid. The second round reverses the
scenario order, so a difference that follows the machine rather than the driver
flips sign. Results land under `bench/out/cluster-4host/` (`OUT_DIR` overrides),
one log and one downloaded directory per round.

Every `CLUSTER_*` and `*_CPU` setting in `hosts.env` is a default that the
environment can override before the file is sourced — the ports come out of one
block per member, above the ephemeral range, and the cores are the layout the
2026-11-02 run used.

## What a reading needs

The same rules as every other batch (`bench/README.md`, "The rules every cell
keeps"): two rounds that agree in sign, a floor measured in the same session, no
mean at this scale. One more belongs to this topology: **a cell is only a
three-member cell if all three members were alive at the end**. Two of the three
are a quorum, so a cell that sends every message can still have lost a member —
`bench/cluster-arms` records each member's counters and election count for that
reason. This runner leaves that evidence to the harness's own logs, and a cell
whose members are not all named in them is not a reading.

## Where the numbers are

The readings this runner produced are in the maintainer's analysis notes, not in
this repository. The four addresses in `hosts.env` are the examples of the
2026-11-02 run; cloud instances get new addresses when they are stopped, so ask
before reusing them — the examples are the only place they appear, and the
refusal above keeps them out of a run nobody named.
