# leases.py
import time
from dataclasses import dataclass, field


@dataclass
class Lease:
    """Żądanie jednej usługi wobec jednego kanału. Wygasa po `expires_at`
    (monotonic), więc martwa usługa nigdy nie blokuje kanału na zawsze."""
    service: str
    state: bool
    priority: int
    expires_at: float
    kind: str = "level"  # "level" (on/off, trzymane przez reconciler) | "flash" (sprzętowy impuls)
    duration: float | None = None  # tylko dla kind="flash"
    applied_at: float = field(default_factory=time.monotonic)

    def is_expired(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return self.expires_at <= now


class ChannelTable:
    """Per-kanał tabela leasingów. Wyższy priorytet wygrywa; przy remisie wygrywa
    ostatnio zaaplikowany. Brak aktywnego leasingu = None (reconciler ma wtedy
    użyć domyślnego stanu OFF)."""

    def __init__(self):
        self._channels: dict[str, dict[str, Lease]] = {}

    def apply(self, channel: str, lease: Lease):
        self._channels.setdefault(channel, {})[lease.service] = lease

    def clear(self, channel: str, service: str):
        leases = self._channels.get(channel)
        if leases:
            leases.pop(service, None)

    def _purge_expired(self, channel: str, now: float):
        leases = self._channels.get(channel)
        if not leases:
            return
        expired = [svc for svc, lease in leases.items() if lease.is_expired(now)]
        for svc in expired:
            del leases[svc]

    def resolve(self, channel: str, now: float | None = None) -> Lease | None:
        now = time.monotonic() if now is None else now
        self._purge_expired(channel, now)
        leases = self._channels.get(channel)
        if not leases:
            return None
        return max(leases.values(), key=lambda l: (l.priority, l.applied_at))

    def channels(self):
        return list(self._channels.keys())
