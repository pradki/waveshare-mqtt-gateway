import sys
import os
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from counters import RelayCounters


def check(name, cond):
    print(("OK  " if cond else "FAIL"), name)


path = os.path.join(tempfile.mkdtemp(), "counters.json")

c1 = RelayCounters(path)
info1 = c1.note_service_start()
check("first ever start -> restarts=1", info1["restarts"] == 1)
check("first_start set", info1["first_start"] is not None)

# simulate a channel counter existing too, to make sure it doesn't collide with _service key
c1.tick("relay1:ch1", True, 5.0)
c1.save(force=True)

# --- simulate a service restart: brand new process, fresh RelayCounters instance, same file
c2 = RelayCounters(path)
snap_before_note = c2.service_snapshot()
check("restart count persisted across process restart (before note)", snap_before_note["restarts"] == 1)

info2 = c2.note_service_start()
check("second start -> restarts=2", info2["restarts"] == 2)
check("first_start preserved across restarts", info2["first_start"] == info1["first_start"])

# channel counters must be untouched by the restart-counter bookkeeping
fan_snapshot = c2.snapshot("relay1:ch1")
check("channel counter survived restart untouched", fan_snapshot["total_s"] == 5)

# --- third restart, reload again
c3 = RelayCounters(path)
info3 = c3.note_service_start()
check("third start -> restarts=3", info3["restarts"] == 3)

snap = c3.service_snapshot()
check("service_snapshot has uptime_s field", "uptime_s" in snap)

print("done")
