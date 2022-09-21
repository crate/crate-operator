# Crate operator Kubernetes Helm charts

## Charts

Install CrateDB by using the operator.

| Charts                                                                                                                 | Description                                                    |
| -----------------------------------------------------------------------------------------------------------------------| -------------------------------------------------------------- |
| [crate-operator](https://github.com/crate/crate-operator/blob/master/deploy/charts/crate-operator/README.md)           | CrateDB operator.                                              |
| [crate-operator-crds](https://github.com/crate/crate-operator/blob/master/deploy/charts/crate-operator-crds/README.md) | CrateDB Custom Resource Definitions (CRDs) for Crate operator. |

## Usage

Add the repo:
```
helm repo add crate-operator https://crate.github.io/crate-operator
```

List the charts:
```
helm search repo crate-operator
```

Install the chart:
```
helm install crate-operator/<chart_name>
```

See [Helm docs](https://helm.sh/docs/helm) for further usage.
