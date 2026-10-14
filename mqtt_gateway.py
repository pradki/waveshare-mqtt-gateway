# mqtt_gateway.py
import asyncio
import json
import logging
import threading
import time

import paho.mqtt.client as mqtt

import config
from leases import ChannelTable, Lease

LOG = logging.getLogger("mqtt_gateway")

VALID_CMDS = {"on", "off", "flash_on", "flash_off", "toggle"}


class MQTTGateway:
    """
    Wątek MQTT (paho, wzorzec jak w cwu_service/mqtt_controller.py). Odbiera komendy z
    `cave/relay/<module>/<name>/cmd/<service>` gdzie `<name>` to albo `ch<N>` (zawsze
    dostępne, dla każdego kanału) albo skonfigurowany alias (dodatkowa nazwa do tego
    samego kanału) - patrz config.channel_names(). Tłumaczy komendę na Lease i aplikuje do
    właściwej ChannelTable, kluczowanej numerem kanału (przez call_soon_threadsafe, więc
    bez wyścigów z reconcilerem, który też działa w event loopie). Publikacja
    statusu/liczników/diagnostyki idzie przez `publish()`, wywoływane z reconcilera.
    """

    def __init__(self, channel_tables: dict[str, ChannelTable], host: str, port: int):
        self.channel_tables = channel_tables
        self.host = host
        self.port = port
        self.loop: asyncio.AbstractEventLoop | None = None

        self.name_to_channel = {
            module_id: config.channel_names(module_cfg)
            for module_id, module_cfg in config.MODULES.items()
        }

        self.cnt_mqtt_connects = 0
        self.cnt_mqtt_disconnects = 0

        self.client = mqtt.Client(
            client_id=config.MQTT_CLIENT_ID,
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)

    def bind_loop(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def get_stats(self) -> dict:
        return {
            "mqtt_connects": self.cnt_mqtt_connects,
            "mqtt_disconnects": self.cnt_mqtt_disconnects,
            "connected": self.client.is_connected(),
        }

    # ---------------- callbacks (wątek MQTT) ----------------
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.cnt_mqtt_connects += 1
            # Subskrybujemy tylko moduły faktycznie skonfigurowane w tym procesie - nie
            # szeroki wildcard "+/+/cmd/+" - żeby obce/przestarzałe moduły (np. retained
            # komendy spod usuniętej/przemianowanej nazwy modułu) nigdy tu nie dotarły,
            # zamiast dostawać je i dopiero potem odrzucać z ostrzeżeniem.
            for module_id in self.channel_tables:
                topic = f"{config.MQTT_TOPIC_ROOT}/{module_id}/+/cmd/+"
                client.subscribe(topic)
            LOG.info("Connected (#%d), subscribed to modules: %s",
                     self.cnt_mqtt_connects, ", ".join(self.channel_tables) or "(none)")
        else:
            LOG.warning("Connect failed rc=%s", rc)

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        self.cnt_mqtt_disconnects += 1
        LOG.warning("Disconnected (#%d) rc=%s", self.cnt_mqtt_disconnects, reason_code)

    def _on_message(self, client, userdata, message, properties=None):
        parts = message.topic.split("/")
        # cave/relay/<module>/<name>/cmd/<service>
        if len(parts) != 6 or parts[0:2] != config.MQTT_TOPIC_ROOT.split("/") or parts[4] != "cmd":
            LOG.warning("Nieoczekiwany topic: %s", message.topic)
            return
        _, _, module_id, name, _, service = parts

        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except Exception:
            LOG.warning("Niepoprawny JSON na %s: %r", message.topic, message.payload)
            return

        if self.loop is None:
            LOG.warning("Event loop jeszcze nie podpięty, ignoruję komendę")
            return
        self.loop.call_soon_threadsafe(self._apply_command, module_id, name, service, payload)

    # ---------------- obsługa komendy (event loop) ----------------
    def _apply_command(self, module_id: str, name: str, service: str, payload: dict):
        table = self.channel_tables.get(module_id)
        if table is None:
            LOG.warning("Nieznany moduł '%s' w komendzie od %s", module_id, service)
            return

        channel = self.name_to_channel.get(module_id, {}).get(name)
        if channel is None:
            LOG.warning("Nieznany kanał/alias '%s' na module '%s' (od %s)", name, module_id, service)
            return

        cmd = payload.get("cmd")
        if cmd not in VALID_CMDS:
            LOG.warning("Nieznana komenda '%s' od %s dla %s/%s", cmd, service, module_id, name)
            return

        priority = int(payload.get("priority", 0))
        now = time.monotonic()

        if cmd in ("on", "off"):
            ttl = float(payload.get("ttl", config.DEFAULT_LEASE_TTL))
            lease = Lease(service=service, state=(cmd == "on"), priority=priority,
                          expires_at=now + ttl, kind="level")
        elif cmd in ("flash_on", "flash_off"):
            duration = payload.get("duration")
            if duration is None:
                LOG.warning("%s od %s dla %s/%s bez 'duration', ignoruję", cmd, service, module_id, name)
                return
            duration = float(duration)
            if cmd == "flash_on":
                # Po impulsie moduł sam rozwiera cewkę, czyli wraca do stanu domyślnego -
                # nie ma czego posiadać, leasing kończy się razem z impulsem.
                lease = Lease(service=service, state=True, priority=priority,
                              expires_at=now + duration, kind="flash", duration=duration,
                              flash_until=now + duration)
            else:
                # `flash_off` to impuls odwrotny: moduł rozwiera cewkę na `duration`, a potem
                # sam ją zwiera. W tym jest cały sens tej komendy ("przerwij na chwilę pracę
                # urządzenia"), więc stan po impulsie trzymamy jako zwykłe "on" - inaczej
                # reconciler gasiłby to, co sprzęt właśnie celowo załączył. Trzymanie działa
                # jak po komendzie `on`: przez `ttl`, do odnowienia.
                ttl = float(payload.get("ttl", config.DEFAULT_LEASE_TTL))
                lease = Lease(service=service, state=False, priority=priority,
                              expires_at=now + duration + ttl, kind="flash", duration=duration,
                              flash_until=now + duration, hold_after=True)
        else:  # toggle
            ttl = float(payload.get("ttl", config.DEFAULT_LEASE_TTL))
            current = table.resolve(channel, now)
            new_state = not current.level_state() if current else True
            lease = Lease(service=service, state=new_state, priority=priority,
                          expires_at=now + ttl, kind="level")

        table.apply(channel, lease)
        LOG.info("Lease %s/ch%d (%s) <- %s (%s, prio=%s)", module_id, channel, name, service, cmd, priority)

    # ---------------- publikacja (wywoływane z reconcilera) ----------------
    def publish(self, topic: str, payload: dict, retain: bool = False):
        try:
            self.client.publish(topic, json.dumps(payload), retain=retain)
        except Exception:
            LOG.exception("Publish error na %s", topic)

    # ---------------- lifecycle ----------------
    def start(self):
        if not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            self.client.disconnect()
            self.client.loop_stop()
        except Exception:
            pass

    def _run_loop(self):
        LOG.info("Connecting to MQTT %s:%s", self.host, self.port)
        try:
            # connect_async(), nie connect() - connect() próbuje się połączyć
            # OD RAZU i rzuca wyjątkiem, gdy broker jeszcze nie przyjmuje
            # połączeń (typowe zaraz po reboocie Pi, jeśli mosquitto wstaje
            # choć chwilę wolniej niż ten gateway) - wyjątek nieobsłużony w
            # tym wątku po cichu go zabija, bez żadnego śladu w systemd
            # (proces główny i tak żyje dalej, tylko już nigdy się nie
            # połączy - przekaźniki przestają reagować na komendy, mimo że
            # `systemctl status` pokazuje "active (running)"). connect_async()
            # odkłada pierwszą próbę do loop_forever(), które z
            # retry_first_connection=True faktycznie ponawia - to właśnie ten
            # retry miał obsłużyć ten przypadek, ale nigdy nie dostawał szansy
            # (zaobserwowane naprawdę, po reboocie Pi - dokładnie ten bug
            # sprawił, że komendy przekaźników z caveweb przestały działać).
            self.client.connect_async(self.host, self.port, keepalive=30)
            self.client.loop_forever(retry_first_connection=True)
        except Exception:
            LOG.exception("MQTT loop error")
