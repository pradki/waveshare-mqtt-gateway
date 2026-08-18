# config.example.py
# Skopiuj do config.py i dostosuj do swojej sieci - config.py jest w .gitignore,
# bo zwykle zawiera prawdziwe adresy IP z Twojej sieci lokalnej.
import platform

# === MQTT ===
if platform.system() == "Linux":
    MQTT_BROKER_HOST = "localhost"
else:
    MQTT_BROKER_HOST = "192.168.1.10"

MQTT_BROKER_PORT = 1883
MQTT_CLIENT_ID = "waveshare-mqtt-gateway"
MQTT_TOPIC_ROOT = "cave/relay"

# Domyślny czas życia (TTL) leasingu on/off, gdy usługa nie poda własnego (sekundy).
DEFAULT_LEASE_TTL = 3600

# === Waveshare Modbus RTU/TCP moduły ===
# aliases: nazwa logiczna -> numer kanału (1..channel_count). Kanały są zawsze
# dostępne też jako "ch1".."chN", niezależnie od tego, czy skonfigurujesz alias.
# counted: nazwy (alias albo "chN"), dla których liczymy czas włączenia (today/month/year/total)
MODULES = {
    "relay1": {
        "host": "192.168.1.50",
        "port": 4196,
        "slave_id": 1,
        "channel_count": 30,
        "reconnect_seconds": 5,
        "tcp_timeout": 3.0,
        "aliases": {
            "co_fan": 1,
            "co_feeder": 2,
            "co_pump": 3,
        },
        "counted": ["co_fan", "co_pump"],
    },
}


def channel_names(module_cfg: dict) -> dict[str, int]:
    """Nazwa (do komend) -> numer kanału. Zawsze zawiera 'ch1'..'chN', niezależnie
    od tego, czy skonfigurowano alias - alias to dodatkowa nazwa do tego samego kanału,
    nie zamiennik. Przy konflikcie alias nadpisuje 'chN' (nie powinno się zdarzyć)."""
    names = {f"ch{i}": i for i in range(1, module_cfg["channel_count"] + 1)}
    names.update(module_cfg.get("aliases", {}))
    return names


def primary_names(module_cfg: dict) -> dict[int, str]:
    """Numer kanału -> 'ładna' nazwa do publikacji (alias jeśli skonfigurowany,
    inaczej 'chN'). Używane do wyboru segmentu topicu przy publikacji statusu."""
    names = {i: f"ch{i}" for i in range(1, module_cfg["channel_count"] + 1)}
    for alias, ch in module_cfg.get("aliases", {}).items():
        names[ch] = alias
    return names


# === Logging / stan ===
LOG_DIRECTORY = "/home/sqna/cave/raspberry/waveshare-mqtt-gateway/logs"
COUNTERS_FILE = "/home/sqna/cave/raspberry/waveshare-mqtt-gateway/counters.json"

COUNTERS_AUTOSAVE_SECONDS = 30
RECONCILE_TICK_SECONDS = 1.0
