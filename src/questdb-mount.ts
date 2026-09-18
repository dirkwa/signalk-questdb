// Where QuestDB's data lives, however Signal K itself is deployed.
//
// The plugin's data directory is one thing to Signal K and another to the
// host's container runtime. Bare-metal, they are the same path. With Signal K
// in a container, the runtime cannot see Signal K's path; signalk-container's
// `resolveHostPath()` translates it to what the runtime can mount — for a bind
// mount the exact host path, for a named volume the volume's NAME plus the
// offset of the directory inside it. A volume cannot be mounted from an offset
// (runtimes reject `vol/sub`, and only some honour a subpath option), so the
// volume is mounted whole and QuestDB is pointed at the offset inside it.
// Dropping that offset mounts the whole Signal K volume as QuestDB's data root
// — a database spread over the directory that also holds security.json and
// every other plugin's data.

import path from "node:path";

/** QuestDB's data root inside its own image; its entrypoint honours the env. */
export const QUESTDB_DATA_DIR = "/var/lib/questdb";
/** Where a whole volume is mounted; a subdirectory per volume so two can never
 * collide, and deterministic so the recreate hash stays stable. */
export const VOLUME_MOUNT_ROOT = "/signalk-vols";

/** What `resolveHostPath()` answers: the left side of `-v`, and the offset. */
export interface MountResolution {
  source: string;
  subPath: string;
}

export interface HostPathResolver {
  resolveHostPath?: (absPath: string) => Promise<MountResolution | null>;
}

export interface QuestdbMount {
  /** `ContainerConfig.volumes` entries. */
  volumes: Record<string, string>;
  /** Env the container needs on top of the usual: `QUESTDB_DATA_DIR` when
   * the data root is not the image's default. */
  env: Record<string, string>;
  /** QuestDB's data root inside the container. */
  dataDir: string;
  /** What a purge deletes. */
  wipePath: string;
  /**
   * Who can delete it. `runtime`: `wipePath` is a host path the runtime can
   * bind-mount, so signalk-container's `removeManagedData` deletes it and,
   * where the Signal K user cannot, wipes it from inside the runtime.
   * `signalk`: `wipePath` is Signal K's own path inside a volume, which the
   * runtime cannot mount by that name; only the Signal K process reaches it.
   */
  wipe: "runtime" | "signalk";
  /**
   * Set for a volume-backed data directory: the Signal K-side path of the
   * volume's root. What QuestDB wrote there before the offset was honoured
   * can be moved into the data directory from the Signal K process, since
   * both are the same volume.
   */
  volumeRootInSignalk?: string;
}

/**
 * Shape the mount for the plugin's data directory.
 *
 * `dataPath` is the directory as Signal K sees it. `resolution` is what the
 * runtime can mount for it.
 */
export function shapeQuestdbMount(
  dataPath: string,
  resolution: MountResolution,
): QuestdbMount {
  if (path.isAbsolute(resolution.source)) {
    // A bind mount, or bare-metal: the host path. signalk-container folds the
    // offset into the source for binds already; joining keeps an older one
    // that reports a parent bind right too, and is a no-op on "".
    const source = path.join(resolution.source, resolution.subPath);
    return {
      volumes: { [QUESTDB_DATA_DIR]: source },
      env: {},
      dataDir: QUESTDB_DATA_DIR,
      wipePath: source,
      wipe: "runtime",
    };
  }
  if (resolution.subPath === "") {
    // A volume mounted exactly at the data directory: mount it as before.
    return {
      volumes: { [QUESTDB_DATA_DIR]: resolution.source },
      env: {},
      dataDir: QUESTDB_DATA_DIR,
      wipePath: dataPath,
      wipe: "signalk",
    };
  }
  const mountDest = `${VOLUME_MOUNT_ROOT}/${resolution.source}`;
  const dataDir = `${mountDest}/${resolution.subPath}`;
  return {
    volumes: { [mountDest]: resolution.source },
    env: { QUESTDB_DATA_DIR: dataDir },
    dataDir,
    wipePath: dataPath,
    wipe: "signalk",
    // The offset is relative to where the volume is mounted in Signal K's
    // container, so that root is the data path with the offset taken off.
    volumeRootInSignalk: dataPath.slice(
      0,
      dataPath.length - resolution.subPath.length - 1,
    ),
  };
}

/**
 * Resolve the mount for the plugin's data directory, falling back to the
 * path itself where nothing can translate it — bare-metal, an older
 * signalk-container without `resolveHostPath`, or one that finds no mount
 * covering the path — since the path is then the host path already.
 */
export async function resolveQuestdbMount(
  resolver: HostPathResolver | undefined,
  dataPath: string,
  debug: (msg: string) => void = () => {},
): Promise<QuestdbMount> {
  const fallback: MountResolution = { source: dataPath, subPath: "" };
  let resolution = fallback;
  if (resolver && typeof resolver.resolveHostPath === "function") {
    // Documented as non-throwing, but reached through a runtime cross-plugin
    // API, so a throw from an unexpected version must not abort startup.
    try {
      resolution = (await resolver.resolveHostPath(dataPath)) ?? fallback;
    } catch (err) {
      debug(
        `resolveHostPath threw, falling back to the data path: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
  return shapeQuestdbMount(dataPath, resolution);
}

/**
 * Delete the data directory from the Signal K process — the purge for a
 * volume, which the runtime cannot mount by Signal K's path. QuestDB's
 * entrypoint makes the directory its own user's, so where that user is not
 * the Signal K user (rootless Podman maps it to a subuid) the delete is
 * refused, and the only way left is by hand.
 */
export async function wipeDataInSignalk(
  rm: (p: string) => Promise<void>,
  dataPath: string,
): Promise<void> {
  try {
    await rm(dataPath);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "EACCES" || code === "EPERM") {
      throw new Error(
        `${dataPath} is owned by QuestDB's user and the Signal K user cannot delete it (${code}); the container is removed, delete the directory by hand`,
        { cause: err },
      );
    }
    throw err;
  }
}

/**
 * The directories QuestDB keeps under its data root, each with a file
 * QuestDB itself writes there. An entry moves only when it carries that
 * file: the names are generic, and a directory of the same name at Signal
 * K's root is somebody else's. `db` — the tables — goes last: it is what
 * marks the root as a database, so a move that stops short still leaves a
 * root that is recognised, and picked up again, on the next start.
 */
export const QUESTDB_ROOT_ENTRIES: ReadonlyArray<{
  name: string;
  mark: string;
}> = [
  { name: "conf", mark: "server.conf" },
  { name: "public", mark: "version.txt" },
  { name: "db", mark: "_tab_index.d" },
];

/** QuestDB writes this into `db` on its first start, tables or not. */
const DB_MARK = path.join("db", "_tab_index.d");

export interface DataDirFs {
  exists(p: string): Promise<boolean>;
  rename(from: string, to: string): Promise<void>;
  mkdir(p: string): Promise<void>;
}

/**
 * Whether a QuestDB database sits at the volume's root — what
 * `adoptDatabaseFromVolumeRoot` moves. Asked first by the caller, which has
 * to stop the container still running on that root before anything moves.
 */
export async function databaseAtVolumeRoot(
  fs: DataDirFs,
  volumeRoot: string,
): Promise<boolean> {
  return fs.exists(path.join(volumeRoot, DB_MARK));
}

/**
 * Move a QuestDB database that sits at a volume's root into the data
 * directory inside that volume.
 *
 * With the whole volume as QuestDB's data root, its database sits at the
 * volume's root — beside security.json. Once the data directory is the root,
 * that database would be left behind and QuestDB would start over empty. Both
 * places are the same volume and the same filesystem to Signal K, so the
 * entries are renamed across, which is instant and does not touch their
 * contents.
 *
 * Only when the root holds a QuestDB database (`db/_tab_index.d`): anything
 * else is not that situation and is left alone. Each entry moves on its own,
 * so a move interrupted part-way is finished by the next call: an entry no
 * longer at the root is done, and `db` moves last. An entry QuestDB has at
 * both places — the tables included — is a duplicate, not an interruption:
 * a completed rename leaves nothing behind, and once `db` has moved nothing
 * would look at the root again. So it is a conflict that fails the start
 * before anything moves. A destination that exists without
 * QuestDB's file is replaced if empty — that is what `rename` does — and a
 * conflict otherwise, which fails the start rather than leaving the
 * database behind unnoticed. Returns the entries moved.
 */
export async function adoptDatabaseFromVolumeRoot(
  fs: DataDirFs,
  volumeRoot: string,
  dataDir: string,
): Promise<string[]> {
  if (!(await databaseAtVolumeRoot(fs, volumeRoot))) return [];
  const pending: { name: string; from: string; to: string }[] = [];
  for (const { name, mark } of QUESTDB_ROOT_ENTRIES) {
    const from = path.join(volumeRoot, name);
    const to = path.join(dataDir, name);
    if (!(await fs.exists(path.join(from, mark)))) continue;
    if (await fs.exists(path.join(to, mark))) {
      throw new Error(
        `QuestDB's ${name} exists at both ${volumeRoot} and ${dataDir}; remove one of them`,
      );
    }
    pending.push({ name, from, to });
  }
  await fs.mkdir(dataDir);
  const moved: string[] = [];
  for (const { name, from, to } of pending) {
    try {
      await fs.rename(from, to);
    } catch (err) {
      throw new Error(
        `QuestDB's ${name} at ${volumeRoot} could not be moved into ${dataDir}: ${err instanceof Error ? err.message : String(err)}`,
        { cause: err },
      );
    }
    moved.push(name);
  }
  return moved;
}
