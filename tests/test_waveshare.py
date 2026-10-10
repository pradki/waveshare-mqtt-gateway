import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncio

from waveshare import Relay30CH, ModbusCommand, crc16


def check(name, cond):
    print(("OK  " if cond else "FAIL"), name)


class FixedResponseClient:
    def __init__(self, response: bytes):
        self.response = response

    async def send(self, frame: bytes) -> bytes:
        return self.response


def full_read_response(slave_id, channel_count, bits):
    byte_count = (channel_count + 7) // 8
    packed = bytearray(byte_count)
    for i, bit in enumerate(bits):
        if bit:
            packed[i // 8] |= (1 << (i % 8))
    frame = bytes([slave_id, 0x01, byte_count]) + bytes(packed)
    return frame + crc16(frame)


async def run():
    # odpowiedź pełna (30 kanałów, ch1 i ch26 ustawione)
    bits = [False] * 30
    bits[0] = True
    bits[25] = True
    good = full_read_response(1, 30, bits)
    relay = Relay30CH(FixedResponseClient(good), slave_id=1, channel_count=30)
    states = await relay.read_all()
    check("pełna odpowiedź: 30 bitów", len(states) == 30)
    check("pełna odpowiedź: ch1 i ch26 odczytane poprawnie",
          states[0] is True and states[25] is True and states[1] is False)

    # odpowiedź ucięta (np. zrośnięta z poprzednią po stronie tcpclient) - za mało
    # bitów na channel_count=30, kiedyś kończyło się IndexErrorem głębiej w
    # reconcilerze zamiast czytelnym błędem tutaj
    short = full_read_response(1, 8, [True] * 8)  # tylko 1 bajt = 8 bitów, nie 30
    relay_short = Relay30CH(FixedResponseClient(short), slave_id=1, channel_count=30)
    try:
        await relay_short.read_all()
        check("ucięta odpowiedź podnosi wyjątek zamiast zwracać krótką listę", False)
    except ValueError as e:
        check("ucięta odpowiedź podnosi ValueError", "bitów" in str(e))

    print("done")


asyncio.run(run())
