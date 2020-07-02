from aioprometheus import Service


class Prometheus:
    _service: Service

    def __init__(self) -> None:
        self._running = False

    def setup(self):
        self._service = Service()

    async def start(self, port: int):
        if not self._running:
            await self._service.start(port=port)
            self._running = True

    async def stop(self):
        if self._running:
            await self._service.stop()
            self._running = False


prometheus = Prometheus()
