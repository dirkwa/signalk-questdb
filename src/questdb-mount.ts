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
  /**
   * What to hand `removeManagedData` on a purge: the exact host path for a
   * bind, and Signal K's own path for a volume, which the Signal K process
   * can delete from since the volume is mounted in its own tree.
   */
  wipePath: string;
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
    };
  }
  if (resolution.subPath === "") {
    // A volume mounted exactly at the data directory: mount it as before.
    return {
      volumes: { [QUESTDB_DATA_DIR]: resolution.source },
      env: {},
      dataDir: QUESTDB_DATA_DIR,
      wipePath: dataPath,
    };
  }
  const mountDest = `${VOLUME_MOUNT_ROOT}/${resolution.source}`;
  const dataDir = `${mountDest}/${resolution.subPath}`;
  return {
    volumes: { [mountDest]: resolution.source },
    env: { QUESTDB_DATA_DIR: dataDir },
    dataDir,
    wipePath: dataPath,
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
 * The directories QuestDB keeps under its data root, in the order they are
 * moved. `db` — the tables — goes last: it is what marks the root as a
 * database, so a move that stops short still leaves a root that is
 * recognised, and picked up again, on the next start.
 */
export const QUESTDB_ROOT_ENTRIES = [
  "conf",
  "public",
  "snapshot",
  ".checkpoint",
  "import",
  "export",
  "profiles",
  "db",
] as const;

/** QuestDB writes this into `db` on its first start, tables or not. */
const DB_SIGNATURE = "_tab_index.d";

export interface DataDirFs {
  exists(p: string): Promise<boolean>;
  rename(from: string, to: string): Promise<void>;
  mkdir(p: string): Promise<void>;
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
 * Only when the root holds a QuestDB database (`db/_tab_index.d`) and the
 * data directory holds none: anything else is not that situation and is left
 * alone. Each entry moves on its own, so a move interrupted part-way is
 * finished by the next call: `db` moves last and is what the root is
 * recognised by, and an entry already at the destination stays where it is.
 * Returns the entries moved.
 */
export async function adoptDatabaseFromVolumeRoot(
  fs: DataDirFs,
  volumeRoot: string,
  dataDir: string,
): Promise<string[]> {
  if (!(await fs.exists(path.join(volumeRoot, "db", DB_SIGNATURE)))) return [];
  if (await fs.exists(path.join(dataDir, "db"))) return [];
  await fs.mkdir(dataDir);
  const moved: string[] = [];
  for (const entry of QUESTDB_ROOT_ENTRIES) {
    const from = path.join(volumeRoot, entry);
    const to = path.join(dataDir, entry);
    if (!(await fs.exists(from)) || (await fs.exists(to))) continue;
    await fs.rename(from, to);
    moved.push(entry);
  }
  return moved;
}
