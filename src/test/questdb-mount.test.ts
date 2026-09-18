import { test, describe } from "node:test";
import assert from "node:assert";
import {
  QUESTDB_DATA_DIR,
  VOLUME_MOUNT_ROOT,
  adoptDatabaseFromVolumeRoot,
  resolveQuestdbMount,
  shapeQuestdbMount,
  type DataDirFs,
} from "../questdb-mount.js";

const DATA = "/home/node/.signalk/plugin-config-data/signalk-questdb";

describe("QuestDB mount shaping", () => {
  // Bare-metal, or an older signalk-container without resolveHostPath: the
  // path Signal K sees is the host path. This is what every install to date
  // produced, so it must stay byte-identical or every upgrade recreates.
  test("a host path mounts at the data root, unchanged", () => {
    const m = shapeQuestdbMount(DATA, { source: DATA, subPath: "" });
    assert.deepStrictEqual(m, {
      volumes: { [QUESTDB_DATA_DIR]: DATA },
      env: {},
      dataDir: QUESTDB_DATA_DIR,
      wipePath: DATA,
    });
  });

  // signalk-container folds the offset into a bind's source itself; an older
  // one that reports the parent bind and an offset joins to the same thing.
  test("a bind reported as parent plus offset joins to the exact host path", () => {
    const m = shapeQuestdbMount(DATA, {
      source: "/srv/signalk-race-config",
      subPath: "plugin-config-data/signalk-questdb",
    });
    assert.deepStrictEqual(m.volumes, {
      [QUESTDB_DATA_DIR]:
        "/srv/signalk-race-config/plugin-config-data/signalk-questdb",
    });
    assert.deepStrictEqual(m.env, {});
    assert.strictEqual(
      m.wipePath,
      "/srv/signalk-race-config/plugin-config-data/signalk-questdb",
    );
    assert.strictEqual(m.volumeRootInSignalk, undefined);
  });

  test("a volume mounted exactly at the data directory mounts as before", () => {
    const m = shapeQuestdbMount(DATA, { source: "questdb-data", subPath: "" });
    assert.deepStrictEqual(m.volumes, { [QUESTDB_DATA_DIR]: "questdb-data" });
    assert.deepStrictEqual(m.env, {});
    assert.strictEqual(m.wipePath, DATA);
  });

  // The reported case: Signal K's whole config directory is a named volume.
  // The volume cannot be mounted from an offset, so it is mounted whole at
  // its own root and QuestDB is pointed inside it — never at the volume's
  // root, which is the whole Signal K config tree.
  test("a volume holding the data directory at an offset is mounted whole and QuestDB pointed inside", () => {
    const name = "ai-sailing-sla2-signalk_signalk-race-config";
    const m = shapeQuestdbMount(DATA, {
      source: name,
      subPath: "plugin-config-data/signalk-questdb",
    });
    assert.deepStrictEqual(m.volumes, {
      [`${VOLUME_MOUNT_ROOT}/${name}`]: name,
    });
    assert.deepStrictEqual(m.env, {
      QUESTDB_DATA_DIR: `${VOLUME_MOUNT_ROOT}/${name}/plugin-config-data/signalk-questdb`,
    });
    assert.strictEqual(m.dataDir, m.env.QUESTDB_DATA_DIR);
    // Signal K's own path is what its process can delete from on a purge.
    assert.strictEqual(m.wipePath, DATA);
    // And the volume's root, as Signal K sees it, is where a database left
    // behind by the old behaviour sits.
    assert.strictEqual(m.volumeRootInSignalk, "/home/node/.signalk");
  });

  test("a resolver that answers nothing, or throws, falls back to the path", async () => {
    const none = await resolveQuestdbMount(
      { resolveHostPath: async () => null },
      DATA,
    );
    assert.deepStrictEqual(none.volumes, { [QUESTDB_DATA_DIR]: DATA });
    const messages: string[] = [];
    const threw = await resolveQuestdbMount(
      {
        resolveHostPath: async () => {
          throw new Error("boom");
        },
      },
      DATA,
      (m) => messages.push(m),
    );
    assert.deepStrictEqual(threw.volumes, { [QUESTDB_DATA_DIR]: DATA });
    assert.ok(messages.some((m) => /boom/.test(m)));
    const absent = await resolveQuestdbMount({}, DATA);
    assert.deepStrictEqual(absent.volumes, { [QUESTDB_DATA_DIR]: DATA });
  });
});

describe("adopting a database from a volume's root", () => {
  // A directory tree as a set of paths; renaming a directory takes what is
  // under it along, as on a filesystem.
  class FakeFs implements DataDirFs {
    present: Set<string>;
    renames: [string, string][] = [];
    made: string[] = [];
    /** Throw on the rename with this index, once — an interruption. */
    failAt = -1;
    constructor(present: string[]) {
      this.present = new Set(present);
    }
    async exists(p: string) {
      return this.present.has(p);
    }
    async rename(from: string, to: string) {
      if (this.renames.length === this.failAt) {
        this.failAt = -1;
        throw new Error(`EIO: rename '${from}'`);
      }
      this.renames.push([from, to]);
      for (const p of [...this.present]) {
        if (p === from || p.startsWith(`${from}/`)) {
          this.present.delete(p);
          this.present.add(`${to}${p.slice(from.length)}`);
        }
      }
    }
    async mkdir(p: string) {
      this.made.push(p);
      this.present.add(p);
    }
  }
  const ROOT = "/home/node/.signalk";
  // What a whole-volume mount left: QuestDB's database at the root of the
  // Signal K volume, next to security.json.
  const OLD_LAYOUT = [
    `${ROOT}/db`,
    `${ROOT}/db/_tab_index.d`,
    `${ROOT}/db/signalk~7`,
    `${ROOT}/conf`,
    `${ROOT}/conf/server.conf`,
    `${ROOT}/public`,
    `${ROOT}/.checkpoint`,
    `${ROOT}/security.json`,
    `${ROOT}/plugin-config-data`,
    `${ROOT}/plugin-config-data/signalk-grafana`,
  ];

  // It moves into the data directory, entry by entry, the tables last, and
  // nothing else at the root is touched.
  test("a database at the volume root moves into the data directory", async () => {
    const fs = new FakeFs(OLD_LAYOUT);
    const moved = await adoptDatabaseFromVolumeRoot(fs, ROOT, DATA);
    assert.deepStrictEqual(moved, ["conf", "public", ".checkpoint", "db"]);
    assert.deepStrictEqual(fs.made, [DATA]);
    assert.deepStrictEqual(fs.renames, [
      [`${ROOT}/conf`, `${DATA}/conf`],
      [`${ROOT}/public`, `${DATA}/public`],
      [`${ROOT}/.checkpoint`, `${DATA}/.checkpoint`],
      [`${ROOT}/db`, `${DATA}/db`],
    ]);
    assert.ok(fs.present.has(`${DATA}/db/signalk~7`));
    assert.ok(fs.present.has(`${ROOT}/security.json`));
    assert.ok(fs.present.has(`${ROOT}/plugin-config-data/signalk-grafana`));
    assert.ok(!fs.present.has(`${ROOT}/db`));
  });

  // A start that fails part-way through the move must not strand the tables
  // at the root: the tables go last and are what the root is recognised by,
  // so the next start carries on where this one stopped.
  test("a move interrupted after its first rename is finished by the next start", async () => {
    const fs = new FakeFs(OLD_LAYOUT);
    fs.failAt = 1;
    await assert.rejects(
      () => adoptDatabaseFromVolumeRoot(fs, ROOT, DATA),
      /EIO/,
    );
    assert.deepStrictEqual(fs.renames, [[`${ROOT}/conf`, `${DATA}/conf`]]);
    assert.ok(fs.present.has(`${ROOT}/db/_tab_index.d`));

    const moved = await adoptDatabaseFromVolumeRoot(fs, ROOT, DATA);
    assert.deepStrictEqual(moved, ["public", ".checkpoint", "db"]);
    assert.deepStrictEqual(fs.renames.slice(1), [
      [`${ROOT}/public`, `${DATA}/public`],
      [`${ROOT}/.checkpoint`, `${DATA}/.checkpoint`],
      [`${ROOT}/db`, `${DATA}/db`],
    ]);
    assert.ok(fs.present.has(`${DATA}/conf/server.conf`));
    assert.ok(fs.present.has(`${DATA}/db/signalk~7`));
    assert.ok(!fs.present.has(`${ROOT}/db`));
    assert.ok(!fs.present.has(`${ROOT}/conf`));
    // And once it is all across, there is nothing left to do.
    assert.deepStrictEqual(
      await adoptDatabaseFromVolumeRoot(fs, ROOT, DATA),
      [],
    );
  });

  test("an entry already at the destination stays where it is", async () => {
    const fs = new FakeFs([
      ...OLD_LAYOUT,
      `${DATA}/conf`,
      `${DATA}/conf/server.conf`,
    ]);
    const moved = await adoptDatabaseFromVolumeRoot(fs, ROOT, DATA);
    assert.deepStrictEqual(moved, ["public", ".checkpoint", "db"]);
    assert.ok(fs.present.has(`${ROOT}/conf/server.conf`));
    assert.ok(fs.present.has(`${DATA}/conf/server.conf`));
    assert.ok(fs.present.has(`${DATA}/db/_tab_index.d`));
  });

  test("a root without a database is left alone", async () => {
    // A `db` directory alone is not a database: QuestDB's own file inside is.
    const fs = new FakeFs([`${ROOT}/security.json`, `${ROOT}/db`]);
    assert.deepStrictEqual(
      await adoptDatabaseFromVolumeRoot(fs, ROOT, DATA),
      [],
    );
    assert.deepStrictEqual(fs.renames, []);
    assert.deepStrictEqual(fs.made, []);
  });

  // Two databases means somebody has already used the data directory; a
  // move would clobber it. Neither is touched.
  test("a data directory that already holds a database is not overwritten", async () => {
    const fs = new FakeFs([
      ...OLD_LAYOUT,
      `${DATA}/db`,
      `${DATA}/db/_tab_index.d`,
    ]);
    assert.deepStrictEqual(
      await adoptDatabaseFromVolumeRoot(fs, ROOT, DATA),
      [],
    );
    assert.deepStrictEqual(fs.renames, []);
    assert.deepStrictEqual(fs.made, []);
  });
});
