from datetime import datetime

from prometheus_client import Gauge, Info

from crate.operator import __version__

i = Info("svc", "Service Info")
i.info(
    {
        "name": "crate-operator",
        "version": __version__,
        "started": datetime.utcnow().isoformat(),
    }
)

cluster_status_gauge = Gauge(
    "cloud_clusters_health",
    documentation="0->GREEN, 1->YELLOW, 2->RED, 3->UNREACHABLE",
    labelnames=["cluster_id"],
)

cluster_last_seen_gauge = Gauge(
    "cloud_clusters_last_seen",
    documentation="Unix timestamp of when a cluster was last seen (not unreachable)",
    labelnames=["cluster_id"],
)
