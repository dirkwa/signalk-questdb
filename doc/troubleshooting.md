# Troubleshooting

## 'Module "signalk-questdb" is not available' right after install or update

Right after installing or updating the plugin, the admin UI can show
`Module "signalk-questdb" is not available. Make sure the webapp is installed.`
It is installed — the browser tab is still running the admin UI it loaded before
the update, which looks for the panel bundle it knew then. A hard refresh of the
tab (Shift-Reload) fixes it.

## "Cannot apply a change: the QuestDB container cannot be stopped or removed"

The plugin needed to recreate the QuestDB container to apply a change — a new
image tag, a network or resource setting, a raised file-descriptor limit — but
the container runtime refused to stop or remove the running container. The
recreate is **deferred, not abandoned**: QuestDB keeps running and keeps
recording, on its **previous** configuration. The plugin reports the error
rather than a "Recording" status, because the change you asked for has not been
applied and reporting success would be a lie.

The usual cause on rootless Podman is an orphaned user namespace left behind
when the Podman service restarted underneath a running container. The error text
carries the specific remedy; the general one is to clear the stale namespace and
remove the container so the plugin can recreate it:

```bash
podman system migrate                     # re-attaches orphaned user namespaces
podman ps -a | grep questdb               # find the managed container's name
podman rm -f <name-from-above>
```

Run all three as the **same OS user that owns the container**. Rootless and
rootful Podman keep separate container stores, so running them as root (or as
another user) will not find, and will not remove, a rootless container.

Look the name up rather than assuming it: the plugin's container is
`signalk-questdb` behind a prefix that defaults to `sk-` but is configurable
(`SIGNALK_CONTAINER_NAMESPACE`), so it is not always `sk-signalk-questdb`.

Then disable and re-enable the plugin, or restart Signal K. The plugin recreates
the container with the settings that were pending. Recorded data lives under
Signal K's data directory, not inside the container (see [Data storage](deployment.md#data-storage)), so
removing the container does not touch it.

**Do not** mask `podman.socket` to work around this. That socket is how the
plugin reaches Podman at all, so masking it turns a deferred recreate into a
container the plugin cannot manage in any way. (`podman.service` is the unit the
socket activates, and masking that has the same effect.) If the socket itself is
unhealthy, restart it rather than masking it:

```bash
systemctl --user restart podman.socket
```

This error is reported with signalk-container 1.29.0 or later. On older versions
the wedge still happens, but appears only as a repeating drift message in the
Signal K server log with no plugin error to go with it.

## "QuestDB keeps dropping the write connection — the container may be unhealthy or out of memory"

This status appears when the plugin's ILP writer connects to QuestDB, gets
dropped within a few seconds, and retries — repeatedly. It is QuestDB itself
being unhealthy (typically OOM-killed and restart-looping), not a problem with
the plugin or with a single table. The most common cause on a Raspberry Pi or
other low-RAM host is that QuestDB is hitting a memory ceiling. There are two
variants, and the cgroup check below tells them apart.

**1. Is the cgroup `memory` controller delegated?**

```bash
cat /sys/fs/cgroup/cgroup.controllers
#   ...memory...   present  -> memory delegation is available, so the cap can be enforced (see B)
#   memory ABSENT           -> the cap is silently dropped (see A)
```

When Signal K's containers run under rootless Podman, the kernel only enforces a
resource limit whose cgroup controller has been delegated to the user session.
Many distributions delegate `cpu`, `cpuset`, `io`, and `pids` by default but
**not** `memory`. signalk-container drops a limit whose controller is missing
rather than failing the container, so a configured cap can silently have no
effect.

**A. `memory` not delegated — the cap is silently dropped.** QuestDB grows
without bound and the host kernel's OOM killer eventually kills it under
whole-system memory pressure; it restarts and the cycle repeats. Enable memory
delegation on the host (one-time, needs sudo), then **recreate** the QuestDB
container (a restart is not enough — the limit is set at create time):

```bash
sudo mkdir -p /etc/systemd/system/user@.service.d
sudo tee /etc/systemd/system/user@.service.d/delegate.conf >/dev/null <<'EOF'
[Service]
Delegate=cpu cpuset io memory pids
EOF
sudo systemctl daemon-reload
```

`daemon-reload` reloads the drop-in file but does not re-apply `Delegate=` to
the already-running user manager, so the new delegation only takes effect after
the **user session restarts**. On a headless box a reboot is simplest (and is
required anyway if you edit `cmdline.txt` below); otherwise restart the user
manager with `sudo systemctl restart user@$(id -u).service` (this stops all of
that user's containers, so let them come back before recreating QuestDB).

```bash
# On older Raspberry Pi kernels the memory controller is off at boot; add to
# /boot/cmdline.txt (one line) and reboot:
#   cgroup_enable=memory cgroup_memory=1
```

After the session restart, recreate the QuestDB container so the cap applies.

**B. `memory` is delegated — the 768 MB cap is enforced but too tight.** As the
database grows, QuestDB's peak memory (JVM heap plus off-heap memory-mapped
files, which spike during out-of-order merges) exceeds 768 MB and the cgroup
OOM-kills it. Raise **QuestDB memory limit** in the plugin config (e.g. `1g` or
`1.5g`, or empty to remove the cap on a roomier host), which recreates the
container.

**Confirm the diagnosis** (substitute the container's name if the prefix is not
`sk-`):

```bash
podman inspect sk-signalk-questdb \
  --format 'OOMKilled={{.State.OOMKilled}} RestartCount={{.RestartCount}} cap={{.HostConfig.Memory}}'
#   cap=805306368  -> 768 MB cap is in place (variant B)
#   cap=0          -> no cap applied (variant A, or cap intentionally removed)
podman events --since 2h --stream=false --filter container=sk-signalk-questdb \
  --filter event=oom --filter event=died
```

## History queries slow or timing out / "out-of-memory" errors with free RAM

QuestDB memory-maps every partition column file and every pending WAL segment it
touches. The Linux kernel caps how many memory mappings one process may hold
(`vm.max_map_count`), and the stock value on Debian, Ubuntu ≤ 22.04, and RHEL
(65530) is far below what QuestDB recommends (1048576). A fresh database fits
easily; months of daily partitions across the three tables — plus a segment
backlog if a table's WAL is suspended — can exhaust the limit. When that happens
`mmap` fails with out-of-memory errors (errno 12) **even though plenty of RAM is
free**: queries error out or crawl, and the WAL apply job can suspend a table.

The QuestDB Web Console shows the same warning
(`vm.max_map_count limit is too low`), the plugin's config panel shows a banner,
and the server log gets a warning at plugin startup. Fix it in a shell **on the
host machine itself** — not inside the QuestDB container via `podman exec`: the
limit is kernel-global, a container cannot change it, and the QuestDB image has
neither `sudo` nor `/etc/sysctl.d`. Being kernel-global also means this works no
matter how Signal K itself is deployed:

```bash
echo 'vm.max_map_count=1048576' | sudo tee /etc/sysctl.d/99-questdb.conf
sudo sysctl --system
```

This takes effect immediately — no container or host restart needed. See
[QuestDB capacity planning](https://questdb.com/docs/getting-started/capacity-planning/#max-virtual-memory-areas-limit)
for background.

## QuestDB container never starts on macOS (podman machine)

On macOS the QuestDB container is created but may never start. What you see
depends on the signalk-container version: with signalk-container 1.26 or later,
the manager names the problem outright (an open-files limit the host refuses)
and starts the container on the runtime's default limits instead, with this
plugin's config panel showing a **"request rejected by the host"** banner — the
remediation below applies unchanged. Older versions never start the container at
all and show either a misleading **"Permission denied"** (the runtime's error
text contains "operation not permitted") or only a generic **"Unexpected error.
See logs for details."** — and the container log view stays empty, because a
container that never started has no logs.

The cause is the plugin's open-files request. On macOS, Signal K runs on the Mac
while podman runs inside a Fedora CoreOS VM ("podman machine"). The plugin asks
for 1048576 open files, and signalk-container normally clamps that request to
what the host can grant — but from macOS it cannot read the VM's limits, so the
full request reaches the VM, exceeds its default hard limit (524288), and the
OCI runtime refuses to start the container. On current Fedora CoreOS the VM's
`vm.max_map_count` already meets QuestDB's 1048576, so usually only the
file-descriptor limit needs raising — but an older machine image may sit lower,
so check it first:

```bash
podman machine ssh -- sysctl -n vm.max_map_count
```

If that prints less than 1048576, raise it inside the VM before relying on the
file-descriptor fix alone (`podman machine ssh`, then
`sudo tee /etc/sysctl.d/99-signalk-questdb.conf <<< 'vm.max_map_count=1048576'`
and `sudo sysctl --system`).

Raise the file-descriptor limit inside the VM. From a macOS terminal:

```bash
podman machine ssh
```

Inside the VM, add a systemd drop-in for every user manager (the `user@.service`
template covers each user instance, including the machine's `core` user, whose
session runs rootless containers) and the same drop-in for `podman.service` to
cover the rootful connection:

```bash
sudo mkdir -p /etc/systemd/system/user@.service.d /etc/systemd/system/podman.service.d
sudo tee /etc/systemd/system/user@.service.d/nofile.conf >/dev/null <<'EOF'
[Service]
LimitNOFILE=1048576
EOF
sudo cp /etc/systemd/system/user@.service.d/nofile.conf /etc/systemd/system/podman.service.d/nofile.conf
sudo systemctl daemon-reload
exit
```

Back on macOS, restart the VM:

```bash
podman machine stop && podman machine start
```

and verify the new limit is grantable:

```bash
podman run --rm --ulimit nofile=1048576:1048576 docker.io/library/alpine sh -c 'ulimit -n -H'
```

This should print `1048576`. Finally, remove the half-created container so it is
recreated with the full limit — the limit is set at create time, so a restart is
not enough:

```bash
podman rm -f sk-signalk-questdb
```

then restart Signal K — on the next plugin start the container is recreated with
the full limit. (The container manager's **Start** button only starts an
existing container, so it cannot replace this step.)

The verify and `rm` commands run on podman's default connection — normally the
rootless one the plugin uses. Rootless and rootful connections keep separate
container stores, so on a setup with a non-default connection add the same
`--connection <name>` (list them with `podman system connection list`) to both
commands, so they hit the store where `sk-signalk-questdb` actually lives
(`podman ps -a` shows it).
