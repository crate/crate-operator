from aioprometheus import Counter, Gauge, Service


class Prometheus:
    _service: Service

    def __init__(self) -> None:
        self._running = False
        self._clusters_current = Gauge(
            "clusters_current", "The number of clusters currently maintained.",
        )
        self._clusters_deleted_total = Counter(
            "clusters_deleted_total", "The total number of clusters deleted.",
        )
        self._clusters_deployed_total = Counter(
            "clusters_deployed_total",
            "The total number of clusters deployed through the operator.",
        )
        self._clusters_restarted_total = Counter(
            "clusters_restarted_total", "The number of times a cluster was restarted."
        )
        self._clusters_scaled_total = Counter(
            "clusters_scaled_total", "The number of times a cluster was scaled."
        )
        self._clusters_upgraded_total = Counter(
            "clusters_upgraded_total", "The number of times a cluster was upgraded."
        )

    def setup(self):
        self._service = Service()
        self._service.register(self._clusters_current)
        self._service.register(self._clusters_deleted_total)
        self._service.register(self._clusters_deployed_total)
        self._service.register(self._clusters_restarted_total)
        self._service.register(self._clusters_scaled_total)
        self._service.register(self._clusters_upgraded_total)

    async def start(self, port: int):
        if not self._running:
            await self._service.start(port=port)
            self._running = True

    async def stop(self):
        if self._running:
            await self._service.stop()
            self._running = False

    def track_cluster_deleted(self):
        self._clusters_deleted_total.inc({})
        self._clusters_current.dec({})

    def track_cluster_deployed(self):
        self._clusters_deployed_total.inc({})
        self._clusters_current.inc({})

    def track_cluster_resume_handling(self):
        self._clusters_current.inc({})

    def track_clusters_restarted_total(self, namespace: str, name: str):
        self._clusters_restarted_total.inc({"namespace": namespace, "name": name})

    def track_clusters_scaled_total(self, namespace: str, name: str):
        self._clusters_scaled_total.inc({"namespace": namespace, "name": name})

    def track_clusters_upgraded_total(self, namespace: str, name: str):
        self._clusters_upgraded_total.inc({"namespace": namespace, "name": name})


prometheus = Prometheus()
