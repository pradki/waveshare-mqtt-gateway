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

    # Tylko dla kind="flash". Impuls sprzętowy trwa do `flash_until`, ale sam leasing może
    # żyć dłużej: `flash_off` kończy się cewką *zwartą* i ten stan trzeba posiadać, a nie
    # tylko przeczekać - stąd `hold_after`. Dla `flash_on` stanem po impulsie jest "off",
    # czyli i tak stan domyślny, więc leasing kończy się razem z impulsem
    # (`flash_until == expires_at`, `hold_after is None`).
    flash_until: float | None = None
    hold_after: bool | None = None

    def __post_init__(self):
        if self.kind == "flash" and self.flash_until is None:
            self.flash_until = self.expires_at

    def is_expired(self, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        return self.expires_at <= now

    def in_flash(self, now: float | None = None) -> bool:
        """Czy sprzętowy impuls jeszcze trwa - wtedy kanał jest hands-off."""
        if self.kind != "flash":
            return False
        now = time.monotonic() if now is None else now
        return now < self.flash_until

    def level_state(self) -> bool:
        """Stan, jaki reconciler ma utrzymywać, gdy impuls się już skończył."""
        if self.kind == "flash":
            return bool(self.hold_after)
        return self.state


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
