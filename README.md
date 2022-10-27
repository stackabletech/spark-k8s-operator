<p align="center">
  <img width="150" src="./.readme/static/borrowed/Icon_Stackable.svg" alt="Stackable Logo"/>
</p>

<h1 align="center">Stackable Operator for Apache Spark</h1>

[![Build Actions Status](https://ci.stackable.tech/job/spark-k8s%2doperator%2dit%2dnightly/badge/icon?subject=Integration%20Tests)](https://ci.stackable.tech/job/spark-k8s%2doperator%2dit%2dnightly)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/stackabletech/spark-k8s-operator/graphs/commit-activity)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-green.svg)](https://docs.stackable.tech/home/stable/contributor/index.html)
[![License OSL3.0](https://img.shields.io/badge/license-OSL3.0-green)](./LICENSE)

[Documentation](https://docs.stackable.tech/spark-k8s/stable/index.html) | [Stackable Data Platform](https://stackable.tech/) | [Platform Docs](https://docs.stackable.tech/) | [Discussions](https://github.com/orgs/stackabletech/discussions)

This is a Kubernetes operator to manage [Apache Spark](https://spark.apache.org/) jobs.

It is part of the Stackable Data Platform, a curated selection of the best open source data apps like Kafka, Druid, Trino or Spark, [all](#other-operators) working together seamlessly. Based on Kubernetes, it runs everywhere – [on prem or in the cloud](#supported-platforms).

## Installation

You can install the operator using [stackablectl or helm](https://docs.stackable.tech/spark-k8s/stable/getting_started/installation.html).

Read on to get started with it, or see it in action in one of our [demos](https://stackable.tech/en/demos/).

## Getting Started

You can follow this [tutorial](https://docs.stackable.tech/spark-k8s/stable/getting_started/first_steps.html).

## Documentation

The stable documentation for this operator can be found [here](https://docs.stackable.tech/spark-k8s/stable/index.html).
If you are interested in the most recent state of this repository, check out the [nightly docs](https://docs.stackable.tech/spark-k8s/nightly/index.html) instead.

The documentation for all Stackable products can be found at [docs.stackable.tech](https://docs.stackable.tech).

If you have a question about the Stackable Data Platform contact us via our [homepage](https://stackable.tech/) or ask a public questions in our [Discussions forum](https://github.com/orgs/stackabletech/discussions).


## About The Stackable Data Platform

This operator is written and maintained by [Stackable](https://www.stackable.tech) and it is part of a larger data platform.

![Stackable Data Platform Overview](./.readme/static/borrowed/sdp_overview.png)

Stackable makes it easy to operate data applications in any Kubernetes cluster.

The data platform offers many operators, new ones being added continuously. All our operators are designed and built to be easily interconnected and to be consistent to work with.

The [Stackable GmbH](https://stackable.tech/) is the company behind the Stackable Data Platform. Offering professional services, paid support plans and custom development.

We love open-source!

## Supported Platforms

We develop and test our operators on the following cloud platforms:

* AKS on Microsoft Azure
* EKS on Amazon Web Services (AWS)
* GKE on Google Cloud Platform (GCP)
* [IONOS Cloud Managed Kubernetes](https://cloud.ionos.com/managed/kubernetes)
* K3s
* Kubernetes 1.21-1.24

We are currently working to support:

* OpenShift

## Other Operators

These are the operators that are currently part of the Stackable Data Platform:

* [Stackable Operator for Apache Airflow](https://github.com/stackabletech/airflow-operator)
* [Stackable Operator for Apache Druid](https://github.com/stackabletech/druid-operator)
* [Stackable Operator for Apache HBase](https://github.com/stackabletech/hbase-operator)
* [Stackable Operator for Apache Hadoop HDFS](https://github.com/stackabletech/hdfs-operator)
* [Stackable Operator for Apache Hive](https://github.com/stackabletech/hive-operator)
* [Stackable Operator for Apache Kafka](https://github.com/stackabletech/kafka-operator)
* [Stackable Operator for Apache NiFi](https://github.com/stackabletech/nifi-operator)
* [Stackable Operator for Apache Spark](https://github.com/stackabletech/spark-k8s-operator)
* [Stackable Operator for Apache Superset](https://github.com/stackabletech/superset-operator)
* [Stackable Operator for Trino](https://github.com/stackabletech/trino-operator)
* [Stackable Operator for Apache ZooKeeper](https://github.com/stackabletech/zookeeper-operator)

And our internal operators:

* [Commons Operator](https://github.com/stackabletech/commons-operator)
* [Listener Operator](https://github.com/stackabletech/listener-operator)
* [OpenPolicyAgent Operator](https://github.com/stackabletech/opa-operator)
* [Secret Operator](https://github.com/stackabletech/secret-operator)

## Contributing

Contributions are welcome. Follow our [Contributors Guide](https://docs.stackable.tech/home/stable/contributor/index.html) to learn how you can contribute.

## License

[Open Software License version 3.0](./LICENSE).

## Support

You can use this project under different licenses. Get started with the community edition! If you want professional support, [we offer subscription plans](https://stackable.tech/en/plans/).
