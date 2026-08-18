import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from leases import ChannelTable, Lease


def check(name, cond):
    print(("OK  " if cond else "FAIL"), name)


t = ChannelTable()
now = 1000.0

# 1. no lease -> None (default OFF)
check("no lease -> None", t.resolve("ch1", now) is None)

# 2. priority wins
t.apply("ch1", Lease(service="a", state=True, priority=5, expires_at=now + 100, applied_at=now))
t.apply("ch1", Lease(service="b", state=False, priority=10, expires_at=now + 100, applied_at=now))
r = t.resolve("ch1", now)
check("higher priority wins (b, off)", r.service == "b" and r.state is False)

# 3. tie-break: same priority, last applied wins
t.apply("ch1", Lease(service="c", state=True, priority=10, expires_at=now + 100, applied_at=now + 1))
r = t.resolve("ch1", now)
check("tie-break: most recent wins (c)", r.service == "c")

# 4. TTL expiry falls back to next highest
r = t.resolve("ch1", now + 200)  # everything expired
check("all expired -> None", r is None)

# 5. expiry falls back to next-highest surviving lease
t2 = ChannelTable()
t2.apply("ch2", Lease(service="low", state=True, priority=1, expires_at=now + 1000))
t2.apply("ch2", Lease(service="high", state=False, priority=20, expires_at=now + 5))
r = t2.resolve("ch2", now)
check("high wins while alive", r.service == "high")
r2 = t2.resolve("ch2", now + 10)  # high expired, low should now win
check("falls back to low after high expires", r2.service == "low")

# 6. flash lease kind preserved
t3 = ChannelTable()
t3.apply("ch3", Lease(service="svc", state=True, priority=10, expires_at=now + 5, kind="flash", duration=5))
r3 = t3.resolve("ch3", now)
check("flash lease kind/duration preserved", r3.kind == "flash" and r3.duration == 5)

print("done")
