# waveshare.py
import struct
from dataclasses import dataclass


def crc16(data: bytes) -> bytes:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return struct.pack('<H', crc)


@dataclass
class ModbusCommand:
    slave_id: int
    function: int
    address: int
    value: int | bytes = 0
    count: int = 0

    def to_bytes(self) -> bytes:
        if self.function in (0x01, 0x02, 0x03, 0x04):
            # Read Coils / Discrete Inputs / Holding / Input Registers
            frame = struct.pack('>B B H H', self.slave_id, self.function, self.address, self.count)
        elif self.function == 0x05:
            # Write Single Coil (używane też do wyzwolenia flash - wtedy value=czas w decysekundach)
            frame = struct.pack('>B B H H', self.slave_id, self.function, self.address, self.value)
        elif self.function == 0x0F:
            # Write Multiple Coils
            nbytes = (self.count + 7) // 8
            frame = struct.pack('>B B H H B', self.slave_id, self.function, self.address, self.count, nbytes)
            frame += self.value
        else:
            raise ValueError(f"Unsupported function: {self.function}")
        return frame + crc16(frame)


class Relay30CH:
    """Waveshare Modbus-RTU 30-kanałowy moduł przekaźnikowy (przez TCP)."""

    def __init__(self, client, slave_id: int = 1, channel_count: int = 30):
        self.client = client
        self.slave_id = slave_id
        self.channel_count = channel_count

    async def relay_on(self, channel: int):
        address = channel - 1
        cmd = ModbusCommand(self.slave_id, 0x05, address, 0xFF00)
        return await self.client.send(cmd.to_bytes())

    async def relay_off(self, channel: int):
        address = channel - 1
        cmd = ModbusCommand(self.slave_id, 0x05, address, 0x0000)
        return await self.client.send(cmd.to_bytes())

    async def relay_toggle(self, channel: int):
        address = channel - 1
        cmd = ModbusCommand(self.slave_id, 0x05, address, 0x5500)
        return await self.client.send(cmd.to_bytes())

    async def relay_flash_on(self, channel: int, duration: float = 1.0):
        """Sprzętowy flash: przekaźnik włącza się i sam wyłączy po `duration` sekund,
        niezależnie od tego, czy ta usługa jeszcze działa."""
        address = (channel - 1) + 0x0200
        cmd = ModbusCommand(self.slave_id, 0x05, address, int(duration * 10))
        return await self.client.send(cmd.to_bytes())

    async def relay_flash_off(self, channel: int, duration: float = 1.0):
        """Sprzętowy flash: przekaźnik wyłącza się i sam włączy po `duration` sekund."""
        address = (channel - 1) + 0x0400
        cmd = ModbusCommand(self.slave_id, 0x05, address, int(duration * 10))
        return await self.client.send(cmd.to_bytes())

    async def read_all(self) -> list[bool]:
        """Odczytuje stan wszystkich kanałów jednym poleceniem (function 0x01)."""
        cmd = ModbusCommand(self.slave_id, 0x01, 0, count=self.channel_count)
        resp = await self.client.send(cmd.to_bytes())
        if len(resp) < 5:
            raise ValueError(f"Odpowiedź za krótka: {resp!r}")
        byte_count = resp[2]
        data_bytes = resp[3:3 + byte_count]
        states = []
        for b in data_bytes:
            for i in range(8):
                states.append(bool(b & (1 << i)))
        if len(states) < self.channel_count:
            # Ramka doszła ucięta albo przesunięta (np. odpowiedź na poprzednie
            # zapytanie dociągnięta z opóźnieniem przez tcpclient i zrośnięta z tą) -
            # niesie mniej bitów niż kanałów. Rzucamy tu, nie niżej: wywołujący już ma
            # na to except (read_all nieudane -> pomiń tick), bez tego kod próbujący
            # zaindeksować states[ch-1] dla wysokich kanałów wywalał się IndexErrorem
            # głębiej w reconcilerze, zamiast dać się złapać w jednym, właściwym miejscu.
            raise ValueError(
                f"Odpowiedź niesie {len(states)} bitów, oczekiwano {self.channel_count}: {resp!r}")
        return states[:self.channel_count]

    async def write_multiple(self, bits: list[bool]):
        """Ustawia wszystkie kanały jednym poleceniem (function 0x0F). `bits[0]` = kanał 1.
        Używane tylko do pełnej resynchronizacji (np. po (re)connect), nie w każdym ticku."""
        if len(bits) != self.channel_count:
            raise ValueError(f"Oczekiwano {self.channel_count} bitów, dostałem {len(bits)}")
        nbytes = (self.channel_count + 7) // 8
        packed = bytearray(nbytes)
        for i, bit in enumerate(bits):
            if bit:
                packed[i // 8] |= (1 << (i % 8))
        cmd = ModbusCommand(self.slave_id, 0x0F, 0, bytes(packed), count=self.channel_count)
        return await self.client.send(cmd.to_bytes())
