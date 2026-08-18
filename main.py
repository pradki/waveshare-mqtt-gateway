# main.py
import asyncio
import logging
import signal

import config
from tcpclient import AsyncTCPClient
from waveshare import Relay30CH
from leases import ChannelTable
from counters import RelayCounters
from mqtt_gateway import MQTTGateway
from reconciler import ModuleReconciler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
LOG = logging.getLogger("main")


async def _gateway_diag_loop(gateway: MQTTGateway, counters: RelayCounters, interval: float):
    """Publikuje diagnostykę samej usługi (ile razy proces się (re)startował, od kiedy
    działa ten konkretny start) - osobno od per-modułowego diag (tcp_connects/disconnects)."""
    topic = f"{config.MQTT_TOPIC_ROOT}/gateway/diag"
    while True:
        payload = {**counters.service_snapshot(), **gateway.get_stats()}
        gateway.publish(topic, payload, retain=True)
        await asyncio.sleep(interval)


async def main():
    channel_tables = {mid: ChannelTable() for mid in config.MODULES}
    gateway = MQTTGateway(channel_tables, config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)

    loop = asyncio.get_running_loop()
    gateway.bind_loop(loop)
    gateway.start()

    counters = RelayCounters(config.COUNTERS_FILE)
    service_info = counters.note_service_start()
    LOG.info("Start usługi #%d (pierwszy start: %s)", service_info["restarts"], service_info["first_start"])

    clients: list[AsyncTCPClient] = []
    reconcilers: list[ModuleReconciler] = []

    for module_id, module_cfg in config.MODULES.items():
        client = AsyncTCPClient(
            host=module_cfg["host"],
            port=module_cfg["port"],
            timeout=module_cfg.get("tcp_timeout", 3.0),
            reconnect=module_cfg.get("reconnect_seconds", 5.0),
        )
        relay = Relay30CH(client, slave_id=module_cfg["slave_id"], channel_count=module_cfg["channel_count"])
        reconciler = ModuleReconciler(
            module_id, module_cfg, relay, channel_tables[module_id], counters, gateway,
            tick_seconds=config.RECONCILE_TICK_SECONDS,
        )
        clients.append(client)
        reconcilers.append(reconciler)
        await client.start()

    stop_event = asyncio.Event()

    def _handle_stop():
        LOG.info("Otrzymano sygnał zatrzymania, zapisuję liczniki...")
        counters.save(force=True)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_stop)
        except (NotImplementedError, AttributeError):
            # Windows (dev) nie wspiera add_signal_handler dla wszystkich sygnałów
            pass

    tasks = [asyncio.create_task(r.run()) for r in reconcilers]
    tasks.append(asyncio.create_task(counters.autosave_loop(config.COUNTERS_AUTOSAVE_SECONDS)))
    tasks.append(asyncio.create_task(_gateway_diag_loop(gateway, counters, config.COUNTERS_AUTOSAVE_SECONDS)))

    try:
        await stop_event.wait()
    finally:
        for t in tasks:
            t.cancel()
        for client in clients:
            await client.stop()
        gateway.stop()
        counters.save(force=True)


if __name__ == "__main__":
    asyncio.run(main())
