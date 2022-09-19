# Crate Operator CRDs Helm Chart

A Helm chart for installing and upgrading the CRDs for [Crate Operator](https://github.com/crate/crate-operator).
To be able to deploy the custom resource CrateDB to a Kubernetes cluster, the API needs to be extended with a Custom Resource Definition (CRD).

Helm must be installed to use the charts. Please refer to Helm's [documentation](https://helm.sh/docs/) to get started.

## Usage

Once Helm is properly set up, add the repo:

```console
helm repo add crate https://
```

### Installing the Operator

```shell
helm install crate-operator-crds crate/crate-operator-crds
```

### Upgrading the Operator

```
helm upgrade --atomic crate-operator-crds crate/crate-operator-crds
```