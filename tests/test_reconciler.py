import sys
import asyncio
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from leases import ChannelTable, Lease
from waveshare import Relay30CH
from reconciler import ModuleReconciler


def check(name, cond):
    print(("OK  " if cond else "FAIL"), name)


class FakeTCPClient:
    """Stand-in for AsyncTCPClient: parses the Modbus frame we'd have sent and
    fakes a device with 4 coils, so we can drive the reconciler without hardware."""
    def __init__(self, channel_count=4):
        self.channel_count = channel_count
        self.coils = [False] * channel_count
        self.cnt_connections = 1
        self.cnt_disconnects = 0
        self.sent = []
        self.flash_fires = []  # (address, seconds) for every 0x05 write to a flash trigger register
        self.fail_single_writes = False  # simulate a channel that refuses to be written

    def get_stats(self):
        return {"tcp_connects": self.cnt_connections, "tcp_disconnects": self.cnt_disconnects}

    async def send(self, frame: bytes) -> bytes:
        self.sent.append(frame)
        function = frame[1]
        if function == 0x01:
            nbytes = (self.channel_count + 7) // 8
            data = bytearray(nbytes)
            for i, v in enumerate(self.coils):
                if v:
                    data[i // 8] |= (1 << (i % 8))
            return bytes([1, 0x01, nbytes]) + bytes(data) + b"\x00\x00"
        elif function == 0x05:
            address = int.from_bytes(frame[2:4], "big")
            value = int.from_bytes(frame[4:6], "big")
            if address < 0x0200:
                if self.fail_single_writes:
                    raise OSError("simulated write failure")
                self.coils[address] = value == 0xFF00
            else:
                self.flash_fires.append((address, value / 10.0))
            return frame  # echo, like a real Modbus write-single-coil ack
        elif function == 0x0F:
            address = int.from_bytes(frame[2:4], "big")
            count = int.from_bytes(frame[4:6], "big")
            nbytes = frame[6]
            data = frame[7:7 + nbytes]
            for i in range(count):
                bit = bool(data[i // 8] & (1 << (i % 8)))
                self.coils[address + i] = bit
            return frame[:6]
        raise ValueError(f"unhandled function {function}")


class FakeGateway:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload, retain=False):
        self.published.append((topic, payload, retain))


class FakeCounters:
    def __init__(self):
        self.ticks = []

    def tick(self, key, is_on, dt):
        self.ticks.append((key, is_on, dt))

    def snapshot(self, key):
        return {"today_s": 0, "month_s": 0, "year_s": 0, "total_s": 0}


def per_channel_status(published):
    # "cave/relay/relay1/<name>/status" (5 segments) vs the module-wide bitmask topic
    # "cave/relay/relay1/status" (4 segments) - both end in "/status", so disambiguate by depth.
    return [p for p in published if p[0].endswith("/status") and p[0].count("/") == 4]


def bitmask_status(published):
    return [p for p in published if p[0].endswith("/status") and p[0].count("/") == 3]


async def run():
    module_cfg = {
        "host": "fake", "port": 0, "slave_id": 1, "channel_count": 4,
        "aliases": {"co_fan": 1, "co_pump": 2, "valve": 3},  # channel 4 stays "ch4" (no alias)
        "counted": ["co_fan"],
    }
    client = FakeTCPClient(channel_count=4)
    relay = Relay30CH(client, slave_id=1, channel_count=4)
    table = ChannelTable()
    gateway = FakeGateway()
    counters = FakeCounters()
    r = ModuleReconciler("relay1", module_cfg, relay, table, counters, gateway, tick_seconds=1.0)

    # --- tick 1: nothing claimed -> everything OFF, resync via write_multiple
    await r._tick()
    check("startup resync uses write_multiple (0x0F)", any(f[1] == 0x0F for f in client.sent))
    check("all coils off after first tick (default OFF)", client.coils == [False, False, False, False])
    check("status published for all 4 channels on first tick", len(per_channel_status(gateway.published)) == 4)
    check("module-wide bitmask status published once on first tick", len(bitmask_status(gateway.published)) == 1)

    gateway.published.clear()
    client.sent.clear()

    # --- tick 2: service 'svcA' claims channel 1 (alias co_fan) ON, priority 5
    table.apply(1, Lease(service="svcA", state=True, priority=5, expires_at=time.monotonic() + 3600))
    await r._tick()
    check("co_fan turned on via single relay_on (0x05), not bulk", client.coils[0] is True and not any(f[1] == 0x0F for f in client.sent))
    co_fan_status = [p for p in per_channel_status(gateway.published) if p[0] == "cave/relay/relay1/co_fan/status"]
    check("status published for co_fan (channel=1, on, svcA)",
          len(co_fan_status) == 1 and co_fan_status[0][1]["channel"] == 1
          and co_fan_status[0][1]["state"] == "on" and co_fan_status[0][1]["by"] == "svcA")

    bm = bitmask_status(gateway.published)
    check("bitmask reflects ch1 on (state_bin=0001, state_hex=0x1)",
          bm and bm[-1][1]["state_bin"] == "0001" and bm[-1][1]["state_hex"] == "0x1")

    gateway.published.clear()

    # --- tick 3: lower priority tries to turn channel 1 off -> ignored
    table.apply(1, Lease(service="svcB", state=False, priority=1, expires_at=time.monotonic() + 3600))
    await r._tick()
    check("lower priority off request ignored, still on", client.coils[0] is True)
    check("no status change published (still svcA/on)", len(per_channel_status(gateway.published)) == 0)

    # --- tick 4: flash_on on channel 3 (alias 'valve') -> hardware call made, channel hands-off
    table.apply(3, Lease(service="svcC", state=True, priority=10, expires_at=time.monotonic() + 2, kind="flash", duration=2))
    await r._tick()
    flash_calls = [f for f in client.sent if f[1] == 0x05 and int.from_bytes(f[2:4], "big") >= 0x0200]
    check("hardware flash trigger sent for valve", len(flash_calls) == 1)

    bm = bitmask_status(gateway.published)
    check("bitmask flash_bin reflects channel 3 flashing (0100 / 0x4)",
          bm and bm[-1][1]["flash_bin"] == "0100" and bm[-1][1]["flash_hex"] == "0x4")

    client.sent.clear()
    # tick again immediately: same lease (same applied_at) should NOT re-fire hardware
    await r._tick()
    flash_calls_2 = [f for f in client.sent if f[1] == 0x05 and int.from_bytes(f[2:4], "big") >= 0x0200]
    check("repeated tick with same flash lease does not re-fire hardware", len(flash_calls_2) == 0)

    # --- counters: channel 1 (co_fan) is 'counted' and has been on for several ticks
    fan_ticks_on = [t for t in counters.ticks if t[0] == "relay1:ch1" and t[1] is True]
    check("counters.tick called with is_on=True for relay1:ch1 while claimed", len(fan_ticks_on) >= 1)

    # --- regression: priority-15 preempts priority-10 flash, then expires and priority-10
    # resumes - it must resume with the *remaining* time, not refire its full original duration
    # (this was the live bug: total ended up as 106s instead of the expected 65s).
    table2 = ChannelTable()
    client2 = FakeTCPClient(channel_count=4)
    relay2 = Relay30CH(client2, slave_id=1, channel_count=4)
    gateway2 = FakeGateway()
    r2 = ModuleReconciler("relay1", module_cfg, relay2, table2, FakeCounters(), gateway2, tick_seconds=1.0)

    table2.apply(1, Lease(service="test", state=True, priority=10,
                           expires_at=time.monotonic() + 0.6, kind="flash", duration=0.6))
    await r2._tick()  # test fires first, full 0.6s

    table2.apply(1, Lease(service="test2", state=True, priority=15,
                           expires_at=time.monotonic() + 0.2, kind="flash", duration=0.2))
    await r2._tick()  # test2 preempts, fires full 0.2s (its own first time)

    await asyncio.sleep(0.25)  # test2's 0.2s window has now passed, test is still valid until 0.6s
    await r2._tick()  # test should resume - but only for the remaining ~0.35s, not a fresh 0.6s

    durations = [d for _, d in client2.flash_fires]
    check("flash fired 3 times total (initial, preempt, resume)", len(durations) == 3)
    check("first fire used full requested duration (0.6s)", abs(durations[0] - 0.6) < 0.05)
    check("preempting fire used its own full duration (0.2s)", abs(durations[1] - 0.2) < 0.05)
    check("resume fire used remaining time, NOT a fresh full 0.6s", 0 < durations[2] < 0.5)

    # --- status payload: expires_at should track the live lease; since should track
    # only real transitions, not renewal pings from the same service/priority/state.
    def last_status_payload(gw, name):
        matches = [p for p in gw.published if p[0] == f"cave/relay/relay1/{name}/status"]
        return matches[-1][1] if matches else None

    gateway3 = FakeGateway()
    table3 = ChannelTable()
    client3 = FakeTCPClient(channel_count=4)
    relay3 = Relay30CH(client3, slave_id=1, channel_count=4)
    r3 = ModuleReconciler("relay1", module_cfg, relay3, table3, FakeCounters(), gateway3, tick_seconds=1.0)

    table3.apply(1, Lease(service="pinger", state=True, priority=10,
                           expires_at=time.monotonic() + 0.3, kind="flash", duration=0.3))
    await r3._tick()
    first_payload = last_status_payload(gateway3, "co_fan")
    check("status has expires_at field", first_payload is not None and "expires_at" in first_payload)
    check("status has since field", first_payload is not None and "since" in first_payload)

    await asyncio.sleep(0.05)
    # renewal ping: same service/priority/state, but a clearly longer fresh duration (like a
    # repeated flash_on keep-alive) so the rounded expires_at reliably differs, not just jitter.
    table3.apply(1, Lease(service="pinger", state=True, priority=10,
                           expires_at=time.monotonic() + 1.3, kind="flash", duration=1.3))
    await r3._tick()
    second_payload = last_status_payload(gateway3, "co_fan")

    check("renewal republishes status with a bumped expires_at",
          second_payload["expires_at"] > first_payload["expires_at"])
    check("renewal does NOT reset 'since' (still the original transition time)",
          second_payload["since"] == first_payload["since"])

    # --- regression: a coil that closed behind our back must be corrected from the
    # *polled* state. This is not hypothetical: flash_off is an inverse pulse (measured on
    # the board - it opens the coil for its duration and then closes it), so once such a
    # pulse expires the channel sits closed with no lease behind it. Gating the write on
    # our own memory of the last write meant "off is already applied", so the relay stayed
    # closed forever while both status topics claimed "off".
    gateway4 = FakeGateway()
    table4 = ChannelTable()
    client4 = FakeTCPClient(channel_count=4)
    relay4 = Relay30CH(client4, slave_id=1, channel_count=4)
    counters4 = FakeCounters()
    r4 = ModuleReconciler("relay1", module_cfg, relay4, table4, counters4, gateway4, tick_seconds=1.0)

    await r4._tick()  # startup resync: everything off, and we remember writing off
    check("baseline: nothing claimed, all coils off", client4.coils == [False, False, False, False])

    client4.coils[0] = True  # cewka zwarła się sama, bez żadnego leasingu
    client4.sent.clear()
    gateway4.published.clear()
    counters4.ticks.clear()
    await r4._tick()
    check("coil closed behind our back gets written off again", client4.coils[0] is False)
    corrections = [f for f in client4.sent
                   if f[1] == 0x05 and int.from_bytes(f[2:4], "big") < 0x0200]
    check("correction is a single-coil write, not a bulk resync", len(corrections) == 1)
    ch1_on_ticks = [t for t in counters4.ticks if t[0] == "relay1:ch1" and t[1] is True]
    check("on-time counter charged the channel for the divergence (it polls the coil)",
          len(ch1_on_ticks) == 1)

    client4.sent.clear()
    await r4._tick()
    check("once corrected, no further writes are issued",
          [f for f in client4.sent if f[1] == 0x05] == [])

    # A channel that refuses to be written must keep showing the polled truth, not our wish.
    client4.coils[0] = True
    client4.fail_single_writes = True
    gateway4.published.clear()
    await r4._tick()
    bm = bitmask_status(gateway4.published)
    check("bitmask reports the coil as on while the correction keeps failing",
          bm and bm[-1][1]["state_bin"].endswith("1"))

    client4.fail_single_writes = False
    await r4._tick()
    check("correction succeeds on the next tick and the coil finally opens",
          client4.coils[0] is False)

    print("done")


asyncio.run(run())
