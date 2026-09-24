import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { ChangeGate } from "../change-gate.js";

const HEARTBEAT = 300_000;

describe("ChangeGate", () => {
  it("skips an unchanged value until the heartbeat is due", () => {
    const gate = new ChangeGate(HEARTBEAT);
    assert.equal(gate.isRepeat("a.b", "self", "s", 1, 0), false);
    gate.wrote("a.b", "self", "s", 1, 0);

    assert.equal(gate.isRepeat("a.b", "self", "s", 1, 1_000), true);
    assert.equal(gate.isRepeat("a.b", "self", "s", 1, HEARTBEAT - 1), true);
    assert.equal(gate.isRepeat("a.b", "self", "s", 1, HEARTBEAT), false);
  });

  it("never skips a change", () => {
    const gate = new ChangeGate(HEARTBEAT);
    gate.wrote("a.b", "self", "s", 1, 0);
    assert.equal(gate.isRepeat("a.b", "self", "s", 2, 1_000), false);
    // Text and booleans compare by value too.
    gate.wrote("sw.state", "self", "s", true, 0);
    assert.equal(gate.isRepeat("sw.state", "self", "s", false, 1), false);
    assert.equal(gate.isRepeat("sw.state", "self", "s", true, 1), true);
  });

  it("compares against the last value written, not the last one seen", () => {
    // A change the throttle dropped was never written, so the gate still
    // holds the old value and the next delta carrying the change goes out.
    const gate = new ChangeGate(HEARTBEAT);
    gate.wrote("a.b", "self", "s", 1, 0);
    assert.equal(gate.isRepeat("a.b", "self", "s", 2, 100), false);
    assert.equal(gate.isRepeat("a.b", "self", "s", 2, 200), false);
  });

  it("keeps sources and contexts apart", () => {
    const gate = new ChangeGate(HEARTBEAT);
    gate.wrote("a.b", "self", "gps1", 1, 0);
    assert.equal(gate.isRepeat("a.b", "self", "gps2", 1, 1), false);
    assert.equal(gate.isRepeat("a.b", "self", undefined, 1, 1), false);
    assert.equal(gate.isRepeat("a.b", "vessels.x", "gps1", 1, 1), false);
  });

  it("is off with a heartbeat of 0", () => {
    const gate = new ChangeGate(0);
    gate.wrote("a.b", "self", "s", 1, 0);
    assert.equal(gate.isRepeat("a.b", "self", "s", 1, 1), false);
    assert.equal(gate.size, 0);
  });

  it("forgets a stale path and its leaves, in that context only", () => {
    // Signal K reports a stale path as null. The reading after the gap must
    // be written even when it equals the one before.
    const gate = new ChangeGate(HEARTBEAT);
    gate.wrote("navigation.attitude.roll", "self", "s", 0.1, 0);
    gate.wrote("navigation.attitude.pitch", "self", "s", 0.2, 0);
    gate.wrote("navigation.attitudeX", "self", "s", 3, 0);
    gate.wrote("navigation.attitude.roll", "vessels.x", "s", 0.1, 0);

    gate.forget("navigation.attitude", "self");

    assert.equal(
      gate.isRepeat("navigation.attitude.roll", "self", "s", 0.1, 1),
      false,
    );
    assert.equal(
      gate.isRepeat("navigation.attitude.pitch", "self", "s", 0.2, 1),
      false,
    );
    // A sibling path sharing the prefix, and another vessel, are untouched.
    assert.equal(
      gate.isRepeat("navigation.attitudeX", "self", "s", 3, 1),
      true,
    );
    assert.equal(
      gate.isRepeat("navigation.attitude.roll", "vessels.x", "s", 0.1, 1),
      true,
    );
    assert.equal(gate.size, 2);
  });

  it("forgets every source of a stale path", () => {
    const gate = new ChangeGate(HEARTBEAT);
    gate.wrote("a.b", "self", "gps1", 1, 0);
    gate.wrote("a.b", "self", "gps2", 1, 0);
    gate.forget("a.b", "self");
    assert.equal(gate.size, 0);
  });

  it("clears when a sweep frees too little to stay below the cap", () => {
    // Otherwise each of the next few new keys would trigger another full walk.
    const gate = new ChangeGate(HEARTBEAT, 4);
    gate.wrote("p1", "vessels.a", "s", 1, 0);
    gate.wrote("p2", "vessels.b", "s", 1, HEARTBEAT);
    gate.wrote("p3", "vessels.c", "s", 1, HEARTBEAT);
    gate.wrote("p4", "vessels.d", "s", 1, HEARTBEAT);
    // One expired entry freed leaves 3 of 4: still above the low-water mark.
    gate.wrote("p5", "vessels.e", "s", 1, HEARTBEAT);
    assert.equal(gate.size, 1);
  });

  it("stays bounded, sweeping entries the heartbeat has already released", () => {
    const gate = new ChangeGate(HEARTBEAT, 3);
    gate.wrote("p1", "vessels.a", "s", 1, 0);
    gate.wrote("p2", "vessels.b", "s", 1, 0);
    gate.wrote("p3", "vessels.c", "s", 1, HEARTBEAT);
    // Full: the two entries older than the heartbeat go, the fresh one stays.
    gate.wrote("p4", "vessels.d", "s", 1, HEARTBEAT);
    assert.equal(gate.size, 2);
    assert.equal(gate.isRepeat("p3", "vessels.c", "s", 1, HEARTBEAT + 1), true);

    // All entries fresh and the cap reached: cleared rather than grown.
    gate.wrote("p5", "vessels.e", "s", 1, HEARTBEAT);
    gate.wrote("p6", "vessels.f", "s", 1, HEARTBEAT);
    assert.ok(gate.size <= 3);
  });
});
