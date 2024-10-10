# Rolling restart with `alter cluster decommission`

While working on a cloud issue a small tool was created
to not only _terminate_ a POD by`kubelet` sending a SIGTERM, but by having the ability
to use a preStop Hook and issue a `alter cluster decommission` for that node.

# What does the tool do?

First the decommission settings are configured for the cluster. We assume that
we always want to _force_ decommission - in terms of: If cratedb would come to the
decision that the decommission failed, it would roll it back. In context of terminating
the POD/process in kubernetes, the shutdown cannot be canceled - therefore _force_ is set
on cratedb side.

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
| `--timemout`          | crateDBs default timeout is 7200s - this needs to be correlated to `TerminationGracePeriod`                                                                                                                                                                     |
| `--pid`               | For testing locally only                                                                                                                                                                                                                                        |
| `--hostname`          | Is used to derive the name of the kubernetes statefulset, the _replica number_ of the pod is _stripped_ from it, which returns the sts name. eg. `crate-data-hot-eadf76b5-c634-4f0f-abcc-7442d01cb7dd-0 -> crate-data-hot-eadf76b5-c634-4f0f-abcc-7442d01cb7dd` |
| `--min-availability`  | Either `PRIMARIES`or `FULL`. Please refer to the crateDB documentation.                                                                                                                                                                                         |
