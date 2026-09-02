#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click>=8",
#     "kubernetes>=31",
#     "urllib3>=2",
# ]
# ///
"""Backfill the new external-dns hostname annotation on CrateDB resources.

external-dns v0.22.0 changed its default annotation prefix from
"external-dns.alpha.kubernetes.io/" to "external-dns.kubernetes.io/" with no
fallback to the old one. The operator sets both prefixes on newly created
resources; this script adds the new annotation to existing operator-managed
Services and Ingresses. Run once per Kubernetes cluster. Resources that
already carry the new annotation are skipped, so re-running is safe.
"""

import sys

import click
import urllib3
from kubernetes import client, config
from kubernetes.client.rest import ApiException

ALPHA = "external-dns.alpha.kubernetes.io/hostname"
NEW = "external-dns.kubernetes.io/hostname"
SELECTOR = "app.kubernetes.io/managed-by=crate-operator,app.kubernetes.io/part-of=cratedb"  # noqa


@click.command(help=__doc__)
@click.option("--apply", is_flag=True, help="Patch the resources (default: dry run).")
@click.option("--context", help="kubeconfig context to use (default: current).")
def main(apply: bool, context: str | None) -> None:
    config.load_kube_config(context=context)
    # Whether to verify TLS comes from the kubeconfig; don't also warn about it.
    if not client.Configuration.get_default_copy().verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    _, active = config.list_kube_config_contexts()
    click.echo(f"Context: {context or active['name']}")
    core = client.CoreV1Api()
    networking = client.NetworkingV1Api()
    patched = skipped = failed = 0
    for kind, list_fn, patch_fn in [
        (
            "service",
            core.list_service_for_all_namespaces,
            core.patch_namespaced_service,
        ),
        (
            "ingress",
            networking.list_ingress_for_all_namespaces,
            networking.patch_namespaced_ingress,
        ),
    ]:
        for item in list_fn(label_selector=SELECTOR).items:
            annotations = item.metadata.annotations or {}
            hostname = annotations.get(ALPHA)
            if not hostname:
                continue
            if annotations.get(NEW):
                skipped += 1
                continue
            ref = f"{kind} {item.metadata.namespace}/{item.metadata.name}"
            if not apply:
                click.echo(f"would annotate {ref} with {NEW}={hostname}")
                patched += 1
                continue
            try:
                patch_fn(
                    item.metadata.name,
                    item.metadata.namespace,
                    {"metadata": {"annotations": {NEW: hostname}}},
                )
            except ApiException as e:
                click.echo(f"failed to annotate {ref}: {e.reason}", err=True)
                failed += 1
            else:
                click.echo(f"annotated {ref} with {NEW}={hostname}")
                patched += 1
    if apply:
        click.echo(f"Annotated {patched}, skipped {skipped}, failed {failed}.")
    else:
        click.echo(f"Would annotate {patched}, {skipped} already done. Use --apply.")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
