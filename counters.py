# counters.py
import asyncio
import json
import logging
import os
import time
from datetime import date

LOG = logging.getLogger("counters")

# Klucz zarezerwowany na statystyki samej usługi (kanały to zawsze "<module>:<alias>",
# więc nie ma ryzyka kolizji).
SERVICE_KEY = "_service"


def _default_bucket() -> dict:
    today = date.today()
    return {
        "today_s": 0.0,
        "today_date": today.isoformat(),
        "month_s": 0.0,
        "month": today.strftime("%Y-%m"),
        "year_s": 0.0,
        "year": str(today.year),
        "total_s": 0.0,
    }


class RelayCounters:
    """Zlicza czas włączenia (sekundy) per kanał: dobowy, miesięczny, roczny i total.
    Klucz kanału to zwykle "<module>:<alias>", żeby aliasy z różnych modułów się
    nie zderzały. Trwałość w pliku JSON, autosave okresowo + on-demand (np. przy SIGTERM)."""

    def __init__(self, path: str):
        self.path = path
        self._data: dict[str, dict] = {}
        self._dirty = False
        self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
            LOG.info("Wczytano liczniki z %s (%d kanałów)", self.path, len(self._data))
        except FileNotFoundError:
            self._data = {}
        except Exception:
            LOG.exception("Nie udało się wczytać liczników z %s, startuję od zera", self.path)
            self._data = {}

    def tick(self, key: str, is_on: bool, dt: float):
        bucket = self._data.setdefault(key, _default_bucket())

        today = date.today()
        if bucket.get("today_date") != today.isoformat():
            bucket["today_s"] = 0.0
            bucket["today_date"] = today.isoformat()
            self._dirty = True

        month = today.strftime("%Y-%m")
        if bucket.get("month") != month:
            bucket["month_s"] = 0.0
            bucket["month"] = month
            self._dirty = True

        year = str(today.year)
        if bucket.get("year") != year:
            bucket["year_s"] = 0.0
            bucket["year"] = year
            self._dirty = True

        if is_on and dt > 0:
            bucket["today_s"] += dt
            bucket["month_s"] += dt
            bucket["year_s"] += dt
            bucket["total_s"] = bucket.get("total_s", 0.0) + dt
            self._dirty = True

    def snapshot(self, key: str) -> dict:
        bucket = self._data.get(key, _default_bucket())
        return {
            "today_s": round(bucket["today_s"]),
            "month_s": round(bucket["month_s"]),
            "year_s": round(bucket["year_s"]),
            "total_s": round(bucket["total_s"]),
        }

    def note_service_start(self) -> dict:
        """Wywoływane raz przy starcie procesu: podbija licznik restartów usługi
        (przetrwa restart, bo trzyma się w tym samym pliku co liczniki kanałów)."""
        bucket = self._data.setdefault(SERVICE_KEY, {"restarts": 0})
        bucket["restarts"] = bucket.get("restarts", 0) + 1
        if not bucket.get("first_start"):
            bucket["first_start"] = time.time()
        bucket["last_start"] = time.time()
        self._dirty = True
        self.save(force=True)  # od razu na dysk, żeby restart w pętli (crash-loop) też się liczył
        return dict(bucket)

    def service_snapshot(self) -> dict:
        bucket = self._data.get(SERVICE_KEY, {})
        last_start = bucket.get("last_start")
        return {
            "restarts": bucket.get("restarts", 0),
            "first_start": bucket.get("first_start"),
            "last_start": last_start,
            "uptime_s": round(time.time() - last_start) if last_start else 0,
        }

    def save(self, force: bool = False):
        if not (self._dirty or force):
            return
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = f"{self.path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2)
            os.replace(tmp, self.path)
            self._dirty = False
        except Exception:
            LOG.exception("Nie udało się zapisać liczników do %s", self.path)

    async def autosave_loop(self, interval: float):
        while True:
            await asyncio.sleep(interval)
            self.save()
