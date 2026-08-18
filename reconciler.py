# reconciler.py
import asyncio
import logging
import time

import config
from leases import ChannelTable
from waveshare import Relay30CH
from counters import RelayCounters
from mqtt_gateway import MQTTGateway

LOG = logging.getLogger("reconciler")


def _bits_to_strs(bits: list[bool]) -> tuple[str, str]:
    """bit (i) = kanał (i+1), czyli ch1 = LSB - tak samo jak w ramce Modbus (read_all/
    write_multiple). Zwraca (binarnie MSB-first jako string, hex)."""
    value = 0
    for i, b in enumerate(bits):
        if b:
            value |= (1 << i)
    bin_str = format(value, f"0{len(bits)}b")
    hex_digits = (len(bits) + 3) // 4
    hex_str = f"0x{value:0{hex_digits}X}"
    return bin_str, hex_str


class ModuleReconciler:
    """
    Jeden tick (domyślnie 1s) na moduł Waveshare:
      1. read_all() - stan faktyczny (function 0x01), ziarno dla liczników.
      2. Dla każdego kanału 1..channel_count (niezależnie od tego, czy ma alias - "chN"
         działa zawsze, alias to dodatkowa nazwa do tego samego kanału): leases.resolve()
         -> zwycięski Lease albo None (-> OFF).
         - "flash": odpalane sprzętowo raz na nową komendę (hardware sam się wyłączy),
           kanał na czas trwania jest "hands-off" - reconciler nie dotyka go w level-write.
         - "level": zbierane do docelowej maski, zapisywane tylko jeśli się zmieniło
           (function 0x05 per kanał), albo jednym write_multiple (function 0x0F) po
           (re)connect, żeby zresynchronizować całość jednym poleceniem.
      3. Publikacja statusu per-kanał (na zmianę), zbiorczej maski bitowej modułu (na
         zmianę) i diagnostyki/liczników (okresowo).
    """

    COUNTERS_PUBLISH_SECONDS = 15.0
    DIVERGENCE_WARN_SECONDS = 30.0

    def __init__(self, module_id: str, module_cfg: dict, relay: Relay30CH,
                 channel_table: ChannelTable, counters: RelayCounters, gateway: MQTTGateway,
                 tick_seconds: float = 1.0):
        self.module_id = module_id
        self.cfg = module_cfg
        self.relay = relay
        self.table = channel_table
        self.counters = counters
        self.gateway = gateway
        self.tick_seconds = tick_seconds

        self.channel_count = module_cfg["channel_count"]
        self.primary_name = config.primary_names(module_cfg)  # ch:int -> "co_fan" | "ch3"

        name_to_channel = config.channel_names(module_cfg)
        self.counted_channels = set()
        for name in module_cfg.get("counted", []):
            ch = name_to_channel.get(name)
            if ch is None:
                LOG.warning("counted: nieznana nazwa '%s' w module %s, pomijam", name, module_id)
                continue
            self.counted_channels.add(ch)

        # _last_applied to już tylko pamięć "co sami ostatnio zapisaliśmy", służąca do
        # rozpoznania, że korygowany rozjazd nie jest naszą sprawką (i warto o nim
        # krzyknąć). O tym, czy zapisywać, decyduje odczyt z modułu - patrz _apply_level.
        self._last_applied: dict[int, bool] = {}
        self._divergence_warned: dict[int, float] = {}  # ch -> monotonic ostatniego ostrzeżenia
        self._last_status: dict[int, tuple] = {}  # ch -> (state, by, priority, expires_at) - decyduje o re-publish
        self._last_transition: dict[int, tuple] = {}  # ch -> (state, by, priority) - decyduje o resecie "since"
        self._state_since: dict[int, float] = {}  # ch -> epoch time.time() ostatniej realnej zmiany
        self._flash_active_identity: dict[int, tuple] = {}  # ch -> (service, applied_at) aktualnie odpalone
        self._flash_ever_fired: dict[int, set] = {}  # ch -> {(service, applied_at), ...} widziane wcześniej
        self._flash_until: dict[int, float] = {}

        self._last_tick_time = time.monotonic()
        self._last_seen_connects = -1
        self._need_resync = True
        self._since_counters_publish = 0.0
        self._last_bitmask: tuple | None = None  # (state_bits_tuple, flash_bits_tuple) ostatnio opublikowane

    async def run(self):
        while True:
            try:
                await self._tick()
            except Exception:
                LOG.exception("Błąd w ticku modułu %s", self.module_id)
            await asyncio.sleep(self.tick_seconds)

    async def _tick(self):
        now = time.monotonic()
        dt = now - self._last_tick_time
        self._last_tick_time = now

        connects = self.relay.client.cnt_connections
        if connects != self._last_seen_connects:
            self._last_seen_connects = connects
            self._need_resync = True

        try:
            actual = await self.relay.read_all()
        except Exception as e:
            LOG.warning("read_all nieudane dla %s: %s", self.module_id, e)
            self._publish_diag()
            return

        for ch in self.counted_channels:
            self.counters.tick(f"{self.module_id}:ch{ch}", actual[ch - 1], dt)

        desired_level: dict[int, bool] = {}
        status_updates = []
        flash_bits = [False] * self.channel_count

        for ch in range(1, self.channel_count + 1):
            lease = self.table.resolve(ch, now)

            if lease and lease.kind == "flash":
                identity = (lease.service, lease.applied_at)
                if self._flash_active_identity.get(ch) != identity:
                    seen = self._flash_ever_fired.setdefault(ch, set())
                    if identity in seen:
                        # Ten lease już raz odpalił sprzęt, ale został wtedy przejęty przez
                        # wyższy priorytet - wznawiamy tylko na czas, który mu jeszcze
                        # zostawał, a nie od nowa na pełny czas trwania.
                        fire_duration = max(0.1, lease.expires_at - now)
                    else:
                        fire_duration = lease.duration
                        seen.add(identity)
                    try:
                        if lease.state:
                            await self.relay.relay_flash_on(ch, fire_duration)
                        else:
                            await self.relay.relay_flash_off(ch, fire_duration)
                    except Exception:
                        LOG.exception("relay_flash_on/off nieudane dla %s/ch%d", self.module_id, ch)
                    self._flash_active_identity[ch] = identity
                self._flash_until[ch] = lease.expires_at
                flash_bits[ch - 1] = True
                state_str = "flash_on" if lease.state else "flash_off"
                by, prio = lease.service, lease.priority
            else:
                if self._flash_until.get(ch, 0) > now:
                    flash_bits[ch - 1] = True
                    continue  # hands-off: hardware jeszcze dogrywa flash, nie ingerujemy
                self._flash_until.pop(ch, None)
                self._flash_active_identity.pop(ch, None)
                self._flash_ever_fired.pop(ch, None)
                state = lease.state if lease else False
                desired_level[ch] = state
                state_str = "on" if state else "off"
                by, prio = (lease.service, lease.priority) if lease else (None, None)

            expires_at = round(time.time() + (lease.expires_at - now)) if lease else None

            transition_key = (state_str, by, prio)
            if self._last_transition.get(ch) != transition_key:
                self._last_transition[ch] = transition_key
                self._state_since[ch] = time.time()

            status_key = (state_str, by, prio, expires_at)
            if self._last_status.get(ch) != status_key:
                self._last_status[ch] = status_key
                since = self._state_since.get(ch, time.time())
                status_updates.append((ch, state_str, by, prio, expires_at, since))

        applied = await self._apply_level(desired_level, actual)

        for ch, state_str, by, prio, expires_at, since in status_updates:
            self._publish_status(ch, state_str, by, prio, expires_at, since)

        # `actual` was read at the *start* of this tick, before the writes above - overlay
        # the changes we actually managed to write, so the bitmask reflects the current
        # state and not last tick's. Only successful writes are overlaid: a channel whose
        # write failed, or that diverged behind our back, must keep showing the polled
        # truth instead of what we wished for. Hands-off (flash) channels aren't touched.
        effective = list(actual)
        for ch, state in applied.items():
            effective[ch - 1] = state
        self._publish_bitmask(effective, flash_bits)

        self._since_counters_publish += dt
        if self._since_counters_publish >= self.COUNTERS_PUBLISH_SECONDS:
            self._since_counters_publish = 0.0
            for ch in self.counted_channels:
                self._publish_counters(ch)

        self._publish_diag()

    def _warn_divergence(self, ch: int, actual_state: bool, desired: bool):
        """Głośno o rozjeździe, ale nie co tick - zaklinowany przekaźnik pisałby do
        journala raz na sekundę i zalałby wszystko inne."""
        now = time.monotonic()
        last = self._divergence_warned.get(ch)
        if last is None or now - last >= self.DIVERGENCE_WARN_SECONDS:
            self._divergence_warned[ch] = now
            LOG.warning("%s/ch%d (%s): moduł zgłasza %s, a stan docelowy to %s - koryguję",
                        self.module_id, ch, self.primary_name[ch],
                        "on" if actual_state else "off", "on" if desired else "off")

    async def _apply_level(self, desired_level: dict[int, bool], actual: list[bool]) -> dict[int, bool]:
        """Dociąga kanały do stanu docelowego; zwraca to, co faktycznie zapisano.

        Zapis pomijamy na podstawie stanu *odczytanego z modułu*, a nie własnej pamięci
        tego, co ostatnio zapisaliśmy - tylko wtedy rozjazd, którego nie spowodowaliśmy,
        sam się naprawia w następnym ticku.

        A takie rozjazdy są realne. `flash_off` to impuls *odwrotny*: rozwiera cewkę na
        `duration`, po czym ją **zwiera** (zmierzone na module). Po wygaśnięciu takiego
        impulsu kanał zostaje więc załączony, i to bez żadnego leasingu za sobą. Gdy
        warunek pomijania patrzył na własną pamięć, gateway uznawał, że "off jest już
        zapisane", i nie ruszał kanału - przekaźnik zostawał zwarty w nieskończoność, a
        oba topici statusu pokazywały "off". Jedynym śladem był licznik on-time, bo ten
        od początku liczy z odczytu."""
        if self._need_resync:
            bits = list(actual)  # kanały bez wyliczonego stanu (hands-off) - zostaw jak są
            for ch, state in desired_level.items():
                bits[ch - 1] = state
            try:
                await self.relay.write_multiple(bits)
                self._need_resync = False
                self._last_applied.update(desired_level)
                return dict(desired_level)
            except Exception:
                LOG.exception("write_multiple (resync) nieudane dla %s", self.module_id)
            return {}

        applied: dict[int, bool] = {}
        for ch, state in desired_level.items():
            if actual[ch - 1] == state:
                self._last_applied[ch] = state
                self._divergence_warned.pop(ch, None)
                continue

            if self._last_applied.get(ch) == state:
                # Ten stan już raz zapisaliśmy, a moduł mówi inaczej - ktoś albo coś
                # ruszyło kanał poza nami.
                self._warn_divergence(ch, actual[ch - 1], state)

            try:
                if state:
                    await self.relay.relay_on(ch)
                else:
                    await self.relay.relay_off(ch)
                self._last_applied[ch] = state
                applied[ch] = state
            except Exception:
                LOG.exception("relay_on/off nieudane dla %s/ch%d", self.module_id, ch)
        return applied

    def _publish_status(self, ch: int, state_str: str, by, prio, expires_at, since):
        name = self.primary_name[ch]
        topic = f"{config.MQTT_TOPIC_ROOT}/{self.module_id}/{name}/status"
        payload = {
            "channel": ch, "state": state_str, "by": by, "priority": prio,
            "since": since, "expires_at": expires_at,
        }
        self.gateway.publish(topic, payload, retain=True)

    def _publish_bitmask(self, actual: list[bool], flash_bits: list[bool]):
        key = (tuple(actual), tuple(flash_bits))
        if key == self._last_bitmask:
            return
        self._last_bitmask = key
        state_bin, state_hex = _bits_to_strs(actual)
        flash_bin, flash_hex = _bits_to_strs(flash_bits)
        topic = f"{config.MQTT_TOPIC_ROOT}/{self.module_id}/status"
        payload = {
            "channels": self.channel_count,
            "bit0": "ch1",  # bit(i) w state_bin/flash_bin (licząc od prawej) = kanał i+1
            "state_bin": state_bin,
            "state_hex": state_hex,
            "flash_bin": flash_bin,
            "flash_hex": flash_hex,
        }
        self.gateway.publish(topic, payload, retain=True)

    def _publish_counters(self, ch: int):
        name = self.primary_name[ch]
        topic = f"{config.MQTT_TOPIC_ROOT}/{self.module_id}/{name}/counters"
        payload = self.counters.snapshot(f"{self.module_id}:ch{ch}")
        self.gateway.publish(topic, payload, retain=True)

    def _publish_diag(self):
        topic = f"{config.MQTT_TOPIC_ROOT}/{self.module_id}/diag"
        self.gateway.publish(topic, self.relay.client.get_stats(), retain=True)
