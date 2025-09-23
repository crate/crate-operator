# Rolling restart with `alter cluster decommission`

While working on a cloud issue a small tool was created
to not only _terminate_ a POD by `kubelet` sending a SIGTERM, but by having the ability
to use a preStop Hook and issue a `alter cluster decommission` for that node.

The cratedb Documentation explains the rolling restart process here: https://cratedb.com/docs/guide/admin/upgrade/rolling.html

Please note that due to the nature of using a preStop Hook, the first stop describe in the
documentation is omitted, as we would not be able to reliably detect that the shutdown was
initiated by dc_util. Therefore the _NEW_PRIMARIES_ would not be
reset!

# What does the tool do?

First the decommission settings are configured for the cluster. By default, _force_
decommission is enabled - in terms of: If cratedb would come to the decision that the
decommission failed, it would roll it back. In context of terminating the POD/process
in kubernetes, the shutdown cannot be canceled - therefore _force_ is typically set on
cratedb side. However, this can now be controlled via the `dc-util-graceful-stop`
StatefulSet label or remains true by default.

Before doing that, the STS is checked for the number of replicas configured. This is done
to figure out whether a FULL stop of all PODS in the cratedb Cluster is _scheduled_. In
case of a FULL restart there is **NO** decommission sent to the cluster and the k8s shutdown
continues by sending `SIGTERM`.

For having access to the number of replicas on the sts, additional permission need to be granted
to the ServiceAccount:

```yaml
- apiGroups: ["apps"]
  resources: ["statefulsets"]
  verbs: ["get", "list", "watch"]
```

This needs to be created/setup manually, or by the crate-operator.

When the decommission is sent to the cluster the command almost immediately returns. Nevertheless
cratedb started the decommissioning in the background and we need to wait until cratedb
exit's.
After that control is _returned_ to`kubelet` which continues by sending SIGTERM.
`Termination Grace Period` needs to be set longer than the decomission timeout, as
kubelet is monitoring this timer and would eventually assume the preStop process _hangs_
and continue with _TERMINATING_ the containers/POD.

# How to configure it?
The preStop Hook needs to be configured on the _Statefulset_ by adding something like this
to the cratedb containers configuration:

```yaml

         image: crate:5.8.3
         lifecycle:
          preStop:
            exec:
              command:
              - /bin/sh
              - -c
              - |
                curl -sLO https://github.com/crate/crate-operator/releases/download/dc_util_v1.0.0/dc_util-linux-amd64 && \
                curl -sLO https://github.com/crate/crate-operator/releases/download/dc_util_v1.0.0/dc_util-linux-amd64.sha256 && \
                sha256sum -c dc_util-linux-amd64.sha256 && \
                chmod u+x ./dc_util-linux-amd64 && \
                ./dc_util-linux-amd64 -min-availability PRIMARIES

        terminationGracePeriodSeconds: 7230

```

In this example the binary is loaded from the GH repo as part of the PODs termination process. In case one of this commands fails, `kubelet` continues with the SIGTERM immediately.

Upload the binary to the CDN: `scp ./dc_util root@web:/mnt/data/www/cdn.crate.io/downloads`. It seems the STDOUT messages written in the `prestop` hook do not
end up in the PODs log, which requires to check the cratedb Logs.

# How to build it?

```shell
GOOS=linux go build -o dc_util  dc_util.go
shasum -a 256 dc_util
```

This builds the binary for `Linux` in case you are building it on MacOS. For convenience and to test locally - without running cratedb - as tiny `http_server` is available.

# Command Line
There are a bunch of CLI parameters that can be set to fine-tune the behavior. Most
are used for testing purpose:

| Paramter              | setting |
| --------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--crate-node-prefix` | allows to customize the cratedb node names in the statefulset in case it is not the default `data-hot`. This is not to be confused with the _hostname_!                                                                                                         |
| `--timeout`           | CrateDB's default timeout is 7200s - this is automatically adjusted based on `terminationGracePeriodSeconds` (see below)                                                                                                                                       |
| `--pid`               | For testing locally only                                                                                                                                                                                                                                        |
| `--hostname`          | Is used to derive the name of the kubernetes statefulset, the _replica number_ of the pod is _stripped_ from it, which returns the sts name. eg. `crate-data-hot-eadf76b5-c634-4f0f-abcc-7442d01cb7dd-0 -> crate-data-hot-eadf76b5-c634-4f0f-abcc-7442d01cb7dd` |
| `--min-availability`  | Either `PRIMARIES`, `FULL`, or `NONE`. Can be overridden by StatefulSet labels (see below). Please refer to the crateDB documentation.                                                                                                                        |

# Timeout Logic

The tool automatically determines the appropriate decommission timeout based on the StatefulSet's `terminationGracePeriodSeconds`:

- **Default case (30s or nil)**: Uses `--timeout` flag value (30s is too small for CrateDB decommissioning)
- **Custom terminationGracePeriodSeconds**: Uses `terminationGracePeriodSeconds - 120s` (reserves 120s for shutdown)
- **Minimum safety**: Always enforces minimum 360s timeout regardless of calculated value
- **Logging**: Reports when using derived timeout instead of flag timeout

## Real-world scenarios:
- **Standard deployment** (30s default): Uses `--timeout` flag (e.g., 7200s)
- **Long-running workload** (1800s): Uses 1680s for decommission, keeps 120s for shutdown
- **Short custom period** (300s): Uses 360s minimum (logs the adjustment)
- **Very long period** (3600s): Uses 3480s for decommission

# StatefulSet Label Configuration

The tool can read configuration from StatefulSet labels, overriding CLI parameters:

## Labels:
- **`dc-util-min-availability`**: Sets min-availability (values: `NONE`, `PRIMARIES`, `FULL`)
- **`dc-util-graceful-stop`**: Controls graceful stop force setting (values: `true`, `false`)

## Example StatefulSet with labels:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: crate-data-hot
  labels:
    dc-util-min-availability: "PRIMARIES"
    dc-util-graceful-stop: "false"
spec:
  # ... rest of StatefulSet spec
```

## Behavior:
- **No labels**: Uses CLI parameter values (`--min-availability`, default force=true)
- **Valid labels**: Uses label values, logs the override
- **Invalid labels**: Uses CLI defaults, logs the invalid value
- **Label precedence**: StatefulSet labels override CLI parameters

## Sample Logs
Please note that you will not be able to see the commands log output! It is run in the backgroud by k8s and is not logged
to STDOUT where you would expect them.

```
bash-5.2# ./dc_util-linux-amd64 -min-availability PRIMARIES -timeout 120s
Decommissioner: 2025/10/09 17:16:46 Using in-cluster configuration
Decommissioner: 2025/10/09 17:16:46 Parsing hostname: crate-data-hot-d84c10e6-d8fb-4d10-bf60-f9f2ea919a73-2
Decommissioner: 2025/10/09 17:16:46 Extracted CrateDB node name: data-hot-2
Decommissioner: 2025/10/09 17:16:46 StatefulSet has 3 replicas configured
Decommissioner: 2025/10/09 17:16:46 Using min-availability from StatefulSet label: NONE
Decommissioner: 2025/10/09 17:16:46 Using graceful stop force from StatefulSet label: false
Decommissioner: 2025/10/09 17:16:46 Using timeout derived from terminationGracePeriodSeconds: 780s (terminationGracePeriodSeconds=900s, buffer=120s) instead of flag value: 120s
Decommissioner: 2025/10/09 17:16:46 StatefulSet terminationGracePeriodSeconds: 900s
Decommissioner: 2025/10/09 17:16:48 Decommissioning node data-hot-2 with graceful_stop.timeout of 780s, min_availability=NONE, force=false
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"set global transient \"cluster.graceful_stop.timeout\" = '780s';"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":24.105846}
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"set global transient \"cluster.graceful_stop.force\" = false;"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":18.95872}
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"set global transient \"cluster.graceful_stop.min_availability\"='NONE';"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":13.927663}
Decommissioner: 2025/10/09 17:16:48 Payload: {"stmt":"alter cluster decommission 'data-hot-2'"}
Decommissioner: 2025/10/09 17:16:48 Response from server: {"cols":[],"rows":[[]],"rowcount":1,"duration":3.827284}
Decommissioner: 2025/10/09 17:16:48 Decommission command sent successfully
Decommissioner: 2025/10/09 17:16:48 Process 1 is still running (check count: 0)

```
