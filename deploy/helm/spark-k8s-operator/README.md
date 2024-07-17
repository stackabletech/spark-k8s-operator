<!-- markdownlint-disable MD034 -->
# Helm Chart for Stackable Operator for Apache Spark-on-Kubernetes

This Helm Chart can be used to install Custom Resource Definitions and the Operator for Apache Spark-on-Kubernetes provided by Stackable.

## Requirements

- Create a [Kubernetes Cluster](../Readme.md)
- Install [Helm](https://helm.sh/docs/intro/install/)

## Install the Stackable Operator for Apache Spark-on-Kubernetes

```bash
# From the root of the operator repository
make compile-chart

helm install spark-k8s-operator deploy/helm/spark-k8s-operator
```

## Usage of the CRDs

The usage of this operator and its CRDs is described in the [documentation](https://docs.stackable.tech/spark-k8s/index.html)

The operator has example requests included in the [`/examples`](https://github.com/stackabletech/spark-k8s-operator/tree/main/examples) directory.

## Links

<https://github.com/stackabletech/spark-k8s-operator>
