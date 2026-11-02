// The update the image check after a plugin update runs
// (updateImageAfterPluginUpdate in signalk-container-helper): pull the
// configured floating tag again, then recreate QuestDB on what the pull
// brought. The plugin's state comes in through `deps`, so it runs against a
// fake container manager in tests.

import { matchContainerInfo } from "signalk-container-helper";
import type { ContainerManagerApi } from "signalk-container-helper";

export interface ImageRefreshDeps {
  containers: Pick<
    ContainerManagerApi,
    "pullImage" | "getImageDigest" | "listContainers" | "getState"
  >;
  /** The container's name, as given to signalk-container. */
  name: string;
  /** The image without its tag, such as `questdb/questdb`. */
  image: string;
  /** Runs `fn` under the plugin's lifecycle lock. */
  withLock<T>(fn: () => Promise<T>): Promise<T>;
  /** Whether the start the check belongs to still stands. */
  current(): boolean;
  /** The image tag configured now. */
  configuredTag(): string;
  /** Why no recreate may run now, or null. */
  blocked(): string | null;
  /**
   * Has signalk-container recreate the container on what the pull brought
   * for `tag`, then waits for it and reconnects.
   */
  recreate(tag: string): Promise<void>;
  /** Starts the container on `tag` as a plugin start would, and reconnects. */
  restore(tag: string): Promise<void>;
  debug(msg: string): void;
  error(msg: string): void;
  /** For QuestDB left down: the server log and the status line. */
  reportDown(msg: string): void;
}

const message = (err: unknown) =>
  err instanceof Error ? err.message : String(err);

/**
 * Pulls `tag`, then recreates the container on it under the lifecycle lock.
 * True once the container runs another image than before, even where a
 * later step failed: the check records the plugin version only then, and an
 * unrecorded recreate would run again at every start. False where nothing
 * changed, or where that cannot be told; a throw also means nothing changed.
 */
export async function refreshImage(
  deps: ImageRefreshDeps,
  tag: string,
  signal: AbortSignal,
): Promise<boolean> {
  const { containers } = deps;
  const ended = () => signal.aborted || !deps.current();
  if (ended()) return false;
  const ref = `${deps.image}:${tag}`;
  const stateOf = async (): Promise<string> => {
    try {
      return await containers.getState(deps.name);
    } catch {
      return "unknown";
    }
  };
  // The image the container runs; null where it cannot be told.
  const runningImage = async (): Promise<string | null> => {
    try {
      const live = matchContainerInfo(
        await containers.listContainers(),
        deps.name,
      );
      return live ? await containers.getImageDigest(live.name) : null;
    } catch {
      return null;
    }
  };
  // Outside the lock, so a download holds up no stop, purge or restart.
  await containers.pullImage(ref);
  return deps.withLock(async () => {
    // Asked again under the lock: an update from the button since the check
    // began moved QuestDB to the release it named, and that choice stands.
    if (deps.configuredTag() !== tag) {
      deps.debug(
        `image refresh skipped: the image version is no longer ${tag}`,
      );
      return false;
    }
    const blocked = deps.blocked();
    if (blocked) {
      deps.debug(`image refresh skipped: ${blocked}`);
      return false;
    }
    // A container someone else stopped is left alone, as the watchdog leaves
    // it; what is started again below is only what this run took down.
    const stateBefore = await stateOf();
    if (stateBefore !== "running") {
      deps.debug(`image refresh skipped: the container is ${stateBefore}`);
      return false;
    }
    // Docker Hub may carry the release later than GitHub announces it; the
    // pull then brought the image QuestDB already runs.
    const pulled = await containers.getImageDigest(ref);
    const before = await runningImage();
    if (pulled !== null && pulled === before) {
      deps.debug(`image refresh skipped: ${ref} is the image QuestDB runs`);
      return false;
    }
    // Asked again after the pull and these reads: stop() does not wait for
    // the lock.
    if (ended()) return false;
    // A failure is judged by what it left: one before the container was
    // handed over left the old image, as below.
    let failure: unknown;
    try {
      await deps.recreate(tag);
    } catch (err) {
      failure = err;
    }
    let state = await stateOf();
    if ((state === "missing" || state === "stopped") && !ended()) {
      // Left down, as where a failure stopped the old container but did not
      // remove it, or removed it and did not create the new one: nothing
      // starts it again but a plugin start, which this does instead.
      try {
        await deps.restore(tag);
      } catch (err) {
        // After a stop, the next start's status line is not this run's.
        if (!ended()) {
          deps.reportDown(
            `Updating QuestDB's image left it down` +
              (failure !== undefined ? ` (${message(failure)})` : "") +
              `, and starting it again failed: ${message(err)}. Restart the plugin.`,
          );
        }
        throw failure ?? err;
      }
      state = await stateOf();
    }
    // A wedged container, a refused remove or a pull held back on a link
    // that turned metered all leave the old image. Where either image cannot
    // be read, a change cannot be told and is not counted: the next start
    // then finds QuestDB current, or the pull's image running, and stops.
    const after = state === "running" ? await runningImage() : null;
    const changed = before !== null && after !== null && after !== before;
    if (!changed) {
      if (failure !== undefined) throw failure;
      deps.debug("image refresh deferred: QuestDB does not run the new image");
      return false;
    }
    if (failure !== undefined && !ended()) {
      deps.error(
        `Updating QuestDB's image failed after the container was recreated: ${message(failure)}`,
      );
    }
    return true;
  });
}
