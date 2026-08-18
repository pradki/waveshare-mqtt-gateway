import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mqtt_gateway import MQTTGateway


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

print("done")
