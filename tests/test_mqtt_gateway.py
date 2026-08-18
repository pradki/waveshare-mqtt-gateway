import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# config.py jest gitignorowany (trzyma prawdziwe adresy z LAN), więc testy jadą na
# tracked szablonie - inaczej świeży klon nie umiałby ich uruchomić. Szablon definiuje
# moduł "relay1", tego samego używają testy.
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "config", Path(__file__).resolve().parent.parent / "config.example.py")
_config = importlib.util.module_from_spec(_spec)
sys.modules["config"] = _config
_spec.loader.exec_module(_config)

from mqtt_gateway import MQTTGateway
from leases import ChannelTable


def check(name, cond):
    print(("OK  " if cond else "FAIL"), name)


class FakeMQTTClient:
    def __init__(self):
        self.subscribed = []

    def subscribe(self, topic):
        self.subscribed.append(topic)


# Regression: subscribing to a blanket "+/+/cmd/+" wildcard meant every configured module
# received (and had to warn-and-drop) commands meant for other modules - including stale
# retained commands left over under a renamed/removed module id. Fixed by subscribing only
# to the modules this process actually owns.
gw = MQTTGateway({"relay_test": object(), "relay2": object()}, "localhost", 1883)
fake_client = FakeMQTTClient()
gw._on_connect(fake_client, None, None, 0)

check("subscribes exactly one topic per configured module", len(fake_client.subscribed) == 2)
check("subscribes relay_test's own topic", "cave/relay/relay_test/+/cmd/+" in fake_client.subscribed)
check("subscribes relay2's own topic", "cave/relay/relay2/+/cmd/+" in fake_client.subscribed)
check("does NOT use a blanket +/+/cmd/+ wildcard", "cave/relay/+/+/cmd/+" not in fake_client.subscribed)

# --- flash_on / flash_off: co komenda zostawia po sobie w tabeli leasingów
# flash_on kończy się cewką rozwartą, czyli w stanie domyślnym - nie ma czego posiadać.
# flash_off kończy się cewką *zwartą* (impuls odwrotny, zmierzone na module), więc ten
# stan musi mieć właściciela, inaczej reconciler zgasiłby to, co sprzęt załączył celowo.
table = ChannelTable()
gw2 = MQTTGateway({"relay1": table}, "localhost", 1883)
gw2.name_to_channel = {"relay1": {"ch1": 1}}  # mapowanie ustala test, bez sięgania do configu

gw2._apply_command("relay1", "ch1", "svc", {"cmd": "flash_on", "duration": 5, "priority": 10})
lease = table.resolve(1)
check("flash_on: leasing kończy się razem z impulsem",
      lease.kind == "flash" and lease.flash_until == lease.expires_at)
check("flash_on: po impulsie stanem docelowym jest off",
      lease.hold_after is None and lease.level_state() is False)
check("flash_on: w trakcie impulsu kanał jest hands-off", lease.in_flash() is True)

gw2._apply_command("relay1", "ch1", "svc",
                   {"cmd": "flash_off", "duration": 5, "ttl": 60, "priority": 10})
lease = table.resolve(1)
check("flash_off: impuls trwa krócej niż leasing",
      lease.flash_until < lease.expires_at
      and abs((lease.expires_at - lease.flash_until) - 60) < 0.5)
check("flash_off: po impulsie trzymamy on (tyle, ile ttl)",
      lease.hold_after is True and lease.level_state() is True)
check("flash_off: sam impuls to nadal wyłączenie cewki", lease.state is False)

print("done")
