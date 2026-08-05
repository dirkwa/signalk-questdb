import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "fs/promises";
import { tmpdir } from "os";
import { join } from "path";
import {
  nofileClampSatisfied,
  parseMaxMapCount,
  evaluateMaxMapCount,
  readMaxMapCount,
  RECOMMENDED_MAX_MAP_COUNT,
} from "../host-limits";

describe("parseMaxMapCount", () => {
  it("parses the proc file format (number + trailing newline)", () => {
    assert.equal(parseMaxMapCount("65530\n"), 65530);
  });

  it("parses without a trailing newline", () => {
    assert.equal(parseMaxMapCount("1048576"), 1048576);
  });

  it("rejects garbage", () => {
    assert.equal(parseMaxMapCount(""), null);
    assert.equal(parseMaxMapCount("not a number\n"), null);
    assert.equal(parseMaxMapCount("-1\n"), null);
    assert.equal(parseMaxMapCount("65530 65530\n"), null);
  });
});

describe("evaluateMaxMapCount", () => {
  it("flags the stock Debian default as too low", () => {
    const status = evaluateMaxMapCount(65530);
    assert.equal(status.tooLow, true);
    assert.equal(status.current, 65530);
    assert.equal(status.recommended, RECOMMENDED_MAX_MAP_COUNT);
  });

  it("accepts exactly the recommended value", () => {
    assert.equal(evaluateMaxMapCount(RECOMMENDED_MAX_MAP_COUNT).tooLow, false);
  });

  it("flags one below the recommended value", () => {
    assert.equal(
      evaluateMaxMapCount(RECOMMENDED_MAX_MAP_COUNT - 1).tooLow,
      true,
    );
  });
});

describe("readMaxMapCount", () => {
  it("reads and evaluates a proc-style file", async () => {
    const dir = await mkdtemp(join(tmpdir(), "host-limits-"));
    try {
      const file = join(dir, "max_map_count");
      await writeFile(file, "65530\n");
      const status = await readMaxMapCount(file);
      assert.ok(status);
      assert.equal(status.current, 65530);
      assert.equal(status.tooLow, true);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it("returns null when the file is missing (non-Linux hosts)", async () => {
    const status = await readMaxMapCount("/nonexistent/max_map_count");
    assert.equal(status, null);
  });

  it("returns null on unparseable content instead of a bogus warning", async () => {
    const dir = await mkdtemp(join(tmpdir(), "host-limits-"));
    try {
      const file = join(dir, "max_map_count");
      await writeFile(file, "garbage\n");
      assert.equal(await readMaxMapCount(file), null);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe("nofileClampSatisfied", () => {
  it("is satisfied when soft and hard both meet the request", () => {
    assert.equal(
      nofileClampSatisfied(1048576, { soft: 1048576, hard: 1048576 }),
      true,
    );
    assert.equal(
      nofileClampSatisfied(1048576, { soft: 2097152, hard: 2097152 }),
      true,
    );
  });

  it("is not satisfied while the container still runs on the clamped limit", () => {
    assert.equal(
      nofileClampSatisfied(1048576, { soft: 524288, hard: 524288 }),
      false,
    );
  });

  it("requires the SOFT limit too — that is what bounds fd allocation", () => {
    assert.equal(
      nofileClampSatisfied(1048576, { soft: 1024, hard: 1048576 }),
      false,
    );
  });

  it("treats an unknown live limit as not satisfied (advisory stays)", () => {
    assert.equal(nofileClampSatisfied(1048576, null), false);
  });
});
