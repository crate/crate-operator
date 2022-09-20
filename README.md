# Crate operator Kubernetes Helm charts

## Charts

Install CrateDB by using the operator.

| Charts                  | Description                                                    |
| ------------------------| -------------------------------------------------------------- |
| crate-operator          | CrateDB operator.                                              |
| crate-operator-crds     | CrateDB Custom Resource Definitions (CRDs) for Crate operator. |

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
helm install crate-operator/<chart name>
```

See [Helm docs](https://helm.sh/docs/helm) for further usage.
