# tcpclient.py
import asyncio
import logging
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional

LOG = logging.getLogger("tcpclient")


class AsyncTCPClient:
    """
    Persistent Modbus-RTU-over-TCP client: jedno długożyjące połączenie, żądania
    serializowane w kolejce (Modbus RTU nie ma numerów zapytań, więc odpowiedzi
    trzeba parować z żądaniami po kolejności). Liczy połączenia i rozłączenia,
    żeby dało się to wystawić na MQTT jako diagnostykę.
    """
    MAX_QUEUE = 10
    COLLECT_TIMEOUT = 0.1  # 100ms - zbieranie reszty odpowiedzi po pierwszym bajcie
    MISSING_LONG_TIME = 120

    def __init__(self, host: str, port: int, timeout: float = 3.0, reconnect: float = 5.0):
        self.host, self.port = host, port
        self.timeout = timeout
        self.reconnect = reconnect

        self.cnt_connections = 0
        self.cnt_disconnects = 0
        self.last_error: Optional[str] = None
        self._disconnect_counted = True  # jeszcze nie połączono = liczy się jako "rozłączony"

        self.ttl_receive = time.monotonic()

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._running = False

        self._queue: deque[tuple[bytes, asyncio.Future]] = deque(maxlen=self.MAX_QUEUE)
        self._pending: asyncio.Future | None = None
        self._buffer = bytearray()
        self._collecting = False

        self._tasks = []

    @property
    def running(self):
        return self._running

    @property
    def connected(self):
        return bool(self._writer and not self._writer.is_closing())

    def get_stats(self) -> dict:
        return {
            "tcp_connects": self.cnt_connections,
            "tcp_disconnects": self.cnt_disconnects,
            "connected": self.connected,
            "last_error": self.last_error,
        }

    def _note_disconnect(self, reason: str):
        if not self._disconnect_counted:
            self.cnt_disconnects += 1
            self._disconnect_counted = True
            self.last_error = reason
            LOG.warning("Disconnected (#%d): %s", self.cnt_disconnects, reason)

    async def _connect(self) -> bool:
        try:
            self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
            LOG.info("Connected to %s:%s", self.host, self.port)
            self.cnt_connections += 1
            self._disconnect_counted = False
            self.ttl_receive = time.monotonic()
            return True
        except Exception as e:
            LOG.warning("Connect error to %s:%s: %s", self.host, self.port, e)
            self.last_error = str(e)
            return False

    async def _reconnect_loop(self):
        while self._running:
            stale = (self.ttl_receive + self.MISSING_LONG_TIME) < time.monotonic()
            if not self._writer or self._writer.is_closing() or stale:
                if stale:
                    self._note_disconnect(f"no data for {self.MISSING_LONG_TIME}s, reconnecting")
                    LOG.warning("Reconnecting: nic nie przyszło od %ss", self.MISSING_LONG_TIME)
                if await self._connect():
                    self._tasks = [
                        asyncio.create_task(self._send_loop()),
                        asyncio.create_task(self._receive_loop()),
                    ]
                    self._buffer.clear()
                    self._collecting = False
                else:
                    await asyncio.sleep(self.reconnect)
            else:
                await asyncio.sleep(0.5)

    async def _send_loop(self):
        while self._running and self._writer and not self._writer.is_closing():
            if not self._queue:
                await asyncio.sleep(0.1)
                continue

            data, future = self._queue[0]
            try:
                self._writer.write(data)
                await self._writer.drain()

                self._pending = future
                self._collecting = True
                self._queue.popleft()
                await asyncio.sleep(0.3)
            except Exception as e:
                LOG.warning("Send error: %s", e)
                self._note_disconnect(f"send error: {e}")
                if not future.done():
                    future.set_exception(ConnectionError("Send failed"))
                self._queue.popleft()
                self._writer.close()
                break

    async def _receive_loop(self):
        while self._running and self._reader:
            try:
                timeout = self.COLLECT_TIMEOUT if self._collecting else 3.0
                chunk = await asyncio.wait_for(self._reader.read(1024), timeout=timeout)
                if not chunk:
                    LOG.warning("Connection closed by peer")
                    self._note_disconnect("connection closed by peer")
                    if self._writer:
                        self._writer.close()
                    break

                self._buffer += chunk

            except asyncio.TimeoutError:
                if self._collecting and self._pending:
                    response = bytes(self._buffer)
                    if not self._pending.done():
                        self._pending.set_result(response)
                    self._pending = None
                    self._collecting = False
                    self._buffer.clear()
                continue

            except Exception as e:
                LOG.warning("Receive error: %s", e)
                self._note_disconnect(f"receive error: {e}")
                break

        if self._pending and not self._pending.done():
            self._pending.set_exception(ConnectionError("Connection lost"))
        self._pending = None
        self._collecting = False
        if self._writer:
            self._writer.close()

    async def send(self, data: bytes) -> bytes:
        """Wysyła dane i czeka na odpowiedź (zbiera wszystko w 100ms po pierwszym bajcie)."""
        if not self._running:
            raise RuntimeError("Client not started")

        future = asyncio.Future()

        if len(self._queue) == self.MAX_QUEUE:
            old_data, old_future = self._queue.popleft()
            if not old_future.done():
                old_future.set_exception(TimeoutError("Queue full"))
            LOG.warning("Queue full, dropped oldest request")

        self._queue.append((data, future))

        try:
            r = await asyncio.wait_for(future, self.timeout)
            self.ttl_receive = time.monotonic()
            return r
        except asyncio.TimeoutError:
            if self._pending == future:
                self._pending = None
                self._collecting = False
            self._note_disconnect("send timeout, no response")
            raise TimeoutError(f"No response in {self.timeout}s")

    async def start(self):
        if self._running:
            return
        self._running = True
        asyncio.create_task(self._reconnect_loop())

    async def stop(self):
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

    @asynccontextmanager
    async def lifespan(self):
        await self.start()
        try:
            yield self
        finally:
            await self.stop()
