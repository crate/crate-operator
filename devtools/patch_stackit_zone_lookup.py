#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click>=8",
#     "kubernetes>=31",
#     "urllib3>=2",
# ]
# ///
"""Rewrite the STACKIT zone lookup in existing CrateDB StatefulSets.

The operator writes the crate command only when it creates a StatefulSet, so
clusters created before crate/cloud#3121 keep the bare curl lookup that can put
an HTTP error body into node.attr.zone. This replaces it with the lookup that
create.py generates now. StatefulSets use OnDelete, so pods pick it up on their
next restart. StatefulSets that already have the new lookup are skipped, so
re-running is safe.
"""

import re
import sys

import click
import urllib3
from kubernetes import client, config
from kubernetes.client.rest import ApiException

SELECTOR = "app.kubernetes.io/component=cratedb,app.kubernetes.io/managed-by=crate-operator,app.kubernetes.io/part-of=cratedb"  # noqa
# "ske", "ske1", ... not glued to other letters, so "skeptic" doesn't match.
SKE = re.compile(r"(?<![a-z])ske\d*+(?![a-z])", re.IGNORECASE)
URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone"
OLD = f"-Cnode.attr.zone=$(curl -s '{URL}')"
# Must match ZONE_CURL_OPTS and ZONE_FILTER in crate/operator/create.py.
NEW = (
    "-Cnode.attr.zone=$(curl -sf --max-time 5 --retry 5 --retry-delay 2 "
    f"--retry-connrefused '{URL}' | "
    "awk 'NR == 1 && /^[A-Za-z0-9_-]+$/ && length($0) <= 63 "
    "{ print; found = 1; exit } "
    'END { if (!found) print "lookup-failed" }\')'
)


@click.command(help=__doc__)
@click.option("--kube-context", required=True, help="kubeconfig context to use.")
@click.option("--dry-run", is_flag=True, help="Only print what would be patched.")
def main(kube_context: str, dry_run: bool) -> None:
    # Only STACKIT clusters were created with the bare curl lookup.
    if not SKE.search(kube_context):
        click.echo(
            f"WARNING: context {kube_context} does not look like STACKIT SKE "
            "(no 'ske' in context name). Stopping.",
            err=True,
        )
        sys.exit(2)
    config.load_kube_config(context=kube_context)
    # Whether to verify TLS comes from the kubeconfig; don't also warn about it.
    if not client.Configuration.get_default_copy().verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    click.echo(f"Context: {kube_context}")
    apps = client.AppsV1Api()
    patched = skipped = unknown = failed = 0
    for sts in apps.list_stateful_set_for_all_namespaces(label_selector=SELECTOR).items:
        ref = f"{sts.metadata.namespace}/{sts.metadata.name}"
        crate = next(c for c in sts.spec.template.spec.containers if c.name == "crate")
        command = crate.command
        if NEW in command:
            skipped += 1
            continue
        if OLD not in command:
            zone = [c for c in command if c.startswith("-Cnode.attr.zone=")]
            click.echo(f"not patching {ref}, unexpected zone lookup: {zone}", err=True)
            unknown += 1
            continue
        if dry_run:
            click.echo(f"would patch {ref}")
            patched += 1
            continue
        command = [NEW if c == OLD else c for c in command]
        # command is replaced as a whole, so fail with 409 instead of undoing an
        # operator change (scale, upgrade) made since the list call.
        body = {
            "metadata": {"resourceVersion": sts.metadata.resource_version},
            "spec": {
                "template": {
                    "spec": {"containers": [{"name": "crate", "command": command}]}
                }
            },
        }
        try:
            apps.patch_namespaced_stateful_set(
                sts.metadata.name, sts.metadata.namespace, body
            )
        except ApiException as e:
            click.echo(f"failed to patch {ref}: {e.reason}, re-run", err=True)
            failed += 1
        else:
            click.echo(f"patched {ref}")
            patched += 1
    if dry_run:
        click.echo(
            f"Would patch {patched}, {skipped} already done, {unknown} unexpected."
        )
    else:
        click.echo(
            f"Patched {patched}, skipped {skipped}, unexpected {unknown}, "
            f"failed {failed}. Pods pick up the change on their next restart."
        )
    sys.exit(1 if failed or unknown else 0)


if __name__ == "__main__":
    main()
