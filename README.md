# waveshare-mqtt-gateway

MQTT → Waveshare Modbus-RTU-over-TCP relay bridge, running as a standalone service on a
Raspberry Pi. It owns the one TCP connection to a Waveshare 30-channel relay board, lets any
number of independent services fight over the same relays through MQTT with priority + TTL
arbitration, and republishes live status, on-time counters and connection diagnostics.

It exists because letting every service (heating, hot water, whatever comes next) dial the
relay board directly over TCP doesn't scale: too many concurrent connections, or polling it
too aggressively, risks resetting the board. Routing everything through one broker-facing
process turns "N services holding open sockets" into "N services publishing small JSON
messages", and gives a single place to decide who wins when two services disagree about the
same channel.

```
 ┌──────────────┐   MQTT commands    ┌───────────────────────┐   Modbus RTU/TCP   ┌─────────────────┐
 │ heating svc   │ ──────────────────▶│                       │ ─ read coils (0x01)▶│                 │
 ├──────────────┤                     │  waveshare-mqtt-      │◀─ write coil (0x05)─│  Waveshare 30CH │
 │ hot water svc │ ──────────────────▶│  gateway              │─ write multiple(0x0F)▶│  relay board    │
 ├──────────────┤   MQTT broker       │  (this repo)          │  one persistent TCP  │                 │
 │ ...whatever   │ ──────────────────▶│                       │  connection          │                 │
 └──────────────┘                     └───────────┬───────────┘                     └─────────────────┘
                                                    │ status / counters / diag (retained)
                                                    ▼
                                              MQTT broker
```

## Why priority + TTL leases (the interesting part)

Several unrelated services need to switch the same physical relays, sometimes disagreeing:

1. **Two services, one channel.** If service A says "on" and service B says "off" for the same
   relay, something has to decide who wins - and it can't just be "last write wins" forever,
   or a misbehaving service could permanently steal a channel from everything else.
2. **A dead service must never win forever.** If a service crashes mid-command, its claim on a
   channel has to expire on its own - nobody is coming back to clean it up.
3. **`flash_on`/`flash_off` has to stay hardware.** The relay board can pulse a channel for a
   given duration entirely on its own (register offset `+0x0200`/`+0x0400`, function 5, value
   = duration in deciseconds) - so even if this gateway process hangs, the pulse still ends on
   time. Re-implementing that as a software timer would throw away the one thing that makes it
   fail-safe.

The fix is a per-channel table of **leases**: `{service, state, priority, expires_at, kind}`.
Every tick (1s):

- Expired leases are dropped.
- The highest-priority surviving lease wins; equal priority is broken by whichever was applied
  most recently (this is the expected case for a service re-sending the same `flash_on` as a
  keep-alive ping).
- No lease at all → the channel defaults to **off**.
- A `flash` lease is only ever fired once per *lease instance* on the physical relay - see
  below for why that distinction matters - and while it's active, the channel is "hands-off"
  for the normal on/off bulk write, so the reconciler never fights the hardware pulse timer.

See [`leases.py`](leases.py) for the whole thing - it's deliberately small and has no
dependency on MQTT, asyncio, or the relay driver, so it's unit-tested in isolation
(`tests/test_leases.py`).

### The bug this design had to survive: preemption and resume

Early testing turned up a real bug worth documenting because the fix isn't obvious. Sequence:

1. Service `a` sends `flash_on` (priority 10, duration 55s). It wins, gateway fires the
   hardware pulse for 55s.
2. Moments later, service `b` sends `flash_on` (priority 15, duration 15s) on the *same*
   channel. Higher priority wins - the hardware pulse is re-triggered for 15s (a single-coil
   flash trigger register can only run one countdown at a time, so the new command replaces the
   old one).
3. 15s later, `b`'s lease expires. `a`'s lease (priority 10) is still valid until its original
   55s deadline, so it becomes the winner again.

The naive fix - "re-fire the hardware pulse whenever the winning lease changes" - is wrong: it
would fire `a`'s pulse for a fresh 55s at step 3, so the relay would stay on for ~70s total
instead of the 55s the caller actually asked for. The gateway instead tracks, per channel, the
*identity* of every lease it has ever fired (`service`, `applied_at`): the first time a given
lease wins, it fires the full requested duration; if that same lease wins *again* after being
preempted, it fires only the time remaining until its own `expires_at`. Net effect: the relay
stays on for exactly the union of the requested windows, regardless of how many times a higher
priority claim interrupts it in between. `tests/test_reconciler.py` has a regression test for
exactly this sequence.

## Hardware

Tested against a Waveshare Modbus RTU Relay 30CH module, addressed over its TCP-to-serial
gateway (not raw serial). Channels are 1-indexed in every command/topic; internally that's
`channel - 1` as the 0-based Modbus coil address.

| Function | Modbus function code | Address | Notes |
|---|---|---|---|
| Read all channel states | `0x01` (Read Coils) | `0` .. `channel_count-1` | One frame reads all channels |
| Write one channel | `0x05` (Write Single Coil) | `channel - 1` | `0xFF00`=on, `0x0000`=off |
| Write all channels | `0x0F` (Write Multiple Coils) | `0` | Used only for a full resync after (re)connect |
| Hardware flash-on | `0x05` | `(channel - 1) + 0x0200` | Value = duration in deciseconds |
| Hardware flash-off | `0x05` | `(channel - 1) + 0x0400` | Value = duration in deciseconds |

## Install

```bash
git clone git@github.com:pradki/waveshare-mqtt-gateway.git
cd waveshare-mqtt-gateway
python3 -m venv /home/sqna/venv/waveshare-mqtt-gateway
/home/sqna/venv/waveshare-mqtt-gateway/bin/pip install -r requirements.txt
cp config.example.py config.py   # then edit config.py, see below
sudo cp waveshare-mqtt-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now waveshare-mqtt-gateway
```

Requires Python 3.10+ (the code uses `X | Y` union type hints in dataclass fields).

## Configuration

Everything lives in `config.py` (gitignored - `config.example.py` is the tracked template,
since `config.py` normally has real LAN IP addresses in it).

- **`MODULES`** - one entry per Waveshare board. `aliases` maps a friendly name to a channel
  number; every channel is *also* always addressable as `ch1`..`chN` regardless of whether it
  has an alias - an alias is an extra name, never a replacement.
- **`counted`** - names (alias or `chN`) to track on-time counters for.
- **`DEFAULT_LEASE_TTL`** - how long a plain `on`/`off` command stays valid if the caller
  doesn't specify its own `ttl`. Long enough that ordinary on/off commands don't need a
  heartbeat, short enough that a crashed service eventually lets go.

Adding a second board is just another entry in `MODULES` - each gets its own TCP connection,
reconciler tick, and topic namespace (`cave/relay/<module>/...`).

## MQTT

### Commands

`cave/relay/<module>/<name>/cmd/<service>` (not retained) - `<name>` is an alias or `chN`,
`<service>` is whatever the caller wants to identify itself as (it's what shows up in `by` on
the status topic).

```json
{"cmd": "flash_on", "duration": 5, "priority": 10}
```

| Field | Required for | Meaning |
|---|---|---|
| `cmd` | always | `on`, `off`, `flash_on`, `flash_off`, or `toggle` |
| `priority` | always (defaults to 0) | Higher wins; ties go to whichever request is newer |
| `ttl` | `on`/`off`/`toggle` only | Seconds until this claim expires (default: `DEFAULT_LEASE_TTL`) |
| `duration` | `flash_on`/`flash_off` only | Seconds the hardware pulse runs for - required, no default |

`toggle` flips whatever the channel's current resolved state is (or turns it on if nothing was
claiming it) - included for completeness, not expected to see much use in practice.

### Per-channel status (retained)

`cave/relay/<module>/<name>/status`

```json
{"channel": 1, "state": "flash_on", "by": "test", "priority": 10, "since": 1752600000, "expires_at": 1752600055}
```

`since` only changes on a real transition (state/owner/priority changing) - a renewal ping
from the same service at the same priority bumps `expires_at` without resetting `since`, so it
still answers "how long has this been on continuously".

### Module-wide bitmask status (retained)

`cave/relay/<module>/status` - the whole board's state in one message, plus which channels are
currently in a hardware flash pulse (and therefore hands-off for normal on/off control).

```json
{"channels": 30, "bit0": "ch1", "state_bin": "000000000000000000000000000101", "state_hex": "0x00000005", "flash_bin": "000000000000000000000000000100", "flash_hex": "0x00000004"}
```

Bit `i` (counting from the right) is channel `i+1`, matching the order used in the underlying
Modbus read/write-multiple frames.

### Counters (retained)

`cave/relay/<module>/<name>/counters` - only published for channels listed in that module's
`counted` config, every 15s.

```json
{"today_s": 42, "month_s": 1337, "year_s": 1337, "total_s": 50000}
```

`today_s` resets at local midnight, `month_s`/`year_s` on the month/year rolling over, `total_s`
never resets. All of it is persisted to `COUNTERS_FILE` and survives a service restart.

### Diagnostics (retained)

`cave/relay/<module>/diag` - per-module Modbus/TCP connection health:

```json
{"tcp_connects": 3, "tcp_disconnects": 2, "connected": true, "last_error": "connection closed by peer"}
```

`cave/relay/gateway/diag` - gateway-level health, published every 30s: how many times the
*process itself* has (re)started (survives restarts, same as the counters file), plus the MQTT
broker connection:

```json
{"restarts": 4, "first_start": 1752000000, "last_start": 1752600000, "uptime_s": 3600, "mqtt_connects": 2, "mqtt_disconnects": 1, "connected": true}
```

`restarts` is a service-level counter (how many times the gateway process has come up) and is
independent of `tcp_disconnects` (how many times the connection to the relay board itself has
dropped) - they answer different questions, so they're deliberately not the same number.

## Reliability

- **TTL leases are the watchdog for on/off control.** A crashed service's claim expires on its
  own; there's no separate liveness check needed because every claim already carries its own
  expiry.
- **Flash stays hardware-timed**, so even a fully hung gateway process can't leave a relay
  stuck on past its pulse duration.
- **One persistent TCP connection**, not one per command - the connection auto-reconnects on a
  fixed retry interval, and every reconnect triggers a one-time full resync (`write_multiple`)
  instead of trusting stale in-memory state.
- Every layer that can fail silently has a counter for it: TCP connects/disconnects, MQTT
  broker connects/disconnects, and service restarts - all retained on MQTT so a dashboard that
  subscribes late still sees the last known values immediately.

## Files

| File | Role |
|---|---|
| `config.py` / `config.example.py` | Module/broker/alias configuration |
| `tcpclient.py` | Persistent, reconnecting Modbus-RTU-over-TCP client with connect/disconnect counters |
| `waveshare.py` | Modbus frame encoding + the relay board's command set (`Relay30CH`) |
| `leases.py` | Priority/TTL arbitration core - pure Python, no I/O |
| `counters.py` | On-time counters and the service-restart counter, both persisted to JSON |
| `mqtt_gateway.py` | MQTT thread: parses commands into leases, publishes status/counters/diag |
| `reconciler.py` | The 1s tick that ties it all together per module |
| `main.py` | Entry point (`asyncio.run`) |
| `waveshare-mqtt-gateway.service` | systemd unit |
| `tests/` | Fake-hardware/fake-broker tests - no Modbus board or MQTT broker required |

## Credits

- [paho-mqtt](https://pypi.org/project/paho-mqtt/) (Eclipse Public License) for the MQTT client.

---

Part of [pradki/cave](https://github.com/pradki/cave). Code comments are in Polish.
