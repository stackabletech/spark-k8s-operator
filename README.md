<!-- markdownlint-disable MD041 -->
<p align="center">
  <img width="150" src="./.readme/static/borrowed/Icon_Stackable.svg" alt="Stackable Logo"/>
</p>

<h1 align="center">Stackable Operator for Apache Spark</h1>

<!-- markdownlint-disable MD041 -->
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://GitHub.com/stackabletech/spark-k8s-operator/graphs/commit-activity)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-green.svg)](https://docs.stackable.tech/home/stable/contributor/index.html)
[![License OSL3.0](https://img.shields.io/badge/license-OSL3.0-green)](./LICENSE)

[Documentation](https://docs.stackable.tech/home/stable/spark-k8s) | [Stackable Data Platform](https://stackable.tech/) | [Platform Docs](https://docs.stackable.tech/) | [Stackable Hub](https://hub.stackable.tech/components/spark) | [Discussions](https://github.com/orgs/stackabletech/discussions) | [Discord](https://discord.gg/7kZ3BNnCAF)

This is a Kubernetes operator to manage [Apache Spark](https://spark.apache.org/) jobs.

<!-- markdownlint-disable MD041 MD051 -->
It is part of the Stackable Data Platform (SDP), a set of Kubernetes operators for open source data products such as Apache Kafka®, Apache Spark™, Apache Superset™, Trino and OpenSearch, [all listed below](#other-operators).
You describe each product in a single custom resource, and the operator takes care of running it, including configuration, security with TLS, Kerberos and Open Policy Agent, upgrades and the connections between products.
It runs on Kubernetes and OpenShift, [on premises, in the cloud or on a laptop](#supported-platforms).

## Installation

You can install the operator using [stackablectl or helm](https://docs.stackable.tech/home/stable/spark-k8s/getting_started/installation).

Read on to get started with it, or see it in action in one of our [demos](https://stackable.tech/en/demos/).

## Getting Started

You can follow this [tutorial](https://docs.stackable.tech/home/stable/spark-k8s/getting_started/first_steps) .

<!-- markdownlint-disable MD041 -->
## Documentation

The stable documentation for this operator can be found in our [Stackable Data Platform documentation](https://docs.stackable.tech/home/stable/spark-k8s).
If you are interested in the most recent state of this repository, check out the [nightly docs](https://docs.stackable.tech/home/nightly/spark-k8s) instead.

The documentation for all Stackable products can be found at [docs.stackable.tech](https://docs.stackable.tech).
Release information, the supported product and platform versions, and the reference for every custom resource are on the [Stackable Hub](https://hub.stackable.tech/).

If you have a question about the Stackable Data Platform, contact us via our [homepage](https://stackable.tech/) or ask a public question in our [Discussions forum](https://github.com/orgs/stackabletech/discussions).

<!-- markdownlint-disable MD041 -->
## Stackable operators or Helm charts?

Without SDP, you assemble each product yourself: from the project's own Helm chart where one exists, otherwise from a third-party chart or your own manifests.
Each product then runs, and everything between the products is yours to build and keep working, for example TLS certificates and Kerberos across products, authorization, service discovery, exposing services outside the cluster, and testing upgrades together.
SDP covers that part.
The products in a release are released together, handle cross-cutting concerns such as security, logging, monitoring and lineage the same way, and get upgrades and fixes as one platform on a predictable schedule.
Every image is signed and comes with an SBOM, see [verifying image signatures](https://docs.stackable.tech/home/stable/guides/enabling-verification-of-image-signatures/) and [viewing and verifying SBOMs](https://docs.stackable.tech/home/stable/guides/viewing-and-verifying-sboms/).

If you run a single product, a well-maintained chart exists for it, and you are happy to own its configuration, security and upgrades yourself, that chart can be a reasonable choice.

## About The Stackable Data Platform

This operator is written and maintained by [Stackable](https://stackable.tech) and is part of the Stackable Data Platform.

![The Stackable Data Platform: Apache Superset for visualization; Apache Spark for analytics and AI; Trino, Apache Hive, Apache NiFi and Apache Spark for data processing; Apache Druid (deprecated), Apache HBase with Apache Phoenix, Apache Hadoop HDFS and OpenSearch for storage; Apache NiFi and Apache Kafka for data ingestion; Apache Airflow and Apache ZooKeeper for orchestration; Open Policy Agent for security, Prometheus metrics for monitoring and Vector for logging; all on Kubernetes and OpenShift](./.readme/static/borrowed/stackable_overview.svg)

Underneath the product operators, the commons, secret and listener operators provide what all products share: S3 and authentication settings, TLS certificates and Kerberos keytabs, exposing services outside the cluster and more.

[Stackable GmbH](https://stackable.tech/) is the company behind the Stackable Data Platform and offers professional services, support subscriptions and custom development.

## Supported Platforms

We develop and test our operators on the following platforms:

* AKS on Microsoft Azure
* EKS on Amazon Web Services (AWS)
* GKE on Google Cloud Platform (GCP)
* [IONOS Cloud Managed Kubernetes](https://cloud.ionos.com/managed/kubernetes)
* K3s
* kind, for example on a laptop, as in the [getting started guide](https://docs.stackable.tech/home/stable/getting-started)
* Kubernetes
* Red Hat OpenShift

The Kubernetes and OpenShift versions each release supports are listed on [hub.stackable.tech](https://hub.stackable.tech/releases).

## Other Operators

These are the operators that are currently part of the Stackable Data Platform:

* [Stackable Operator for Apache Airflow](https://github.com/stackabletech/airflow-operator)
* [Stackable Operator for Apache Druid](https://github.com/stackabletech/druid-operator), deprecated since SDP 26.7
* [Stackable Operator for Apache HBase](https://github.com/stackabletech/hbase-operator)
* [Stackable Operator for Apache Hadoop HDFS](https://github.com/stackabletech/hdfs-operator)
* [Stackable Operator for Apache Hive](https://github.com/stackabletech/hive-operator)
* [Stackable Operator for Apache Kafka](https://github.com/stackabletech/kafka-operator)
* [Stackable Operator for Apache NiFi](https://github.com/stackabletech/nifi-operator)
* [Stackable Operator for the Open Policy Agent (OPA)](https://github.com/stackabletech/opa-operator)
* [Stackable Operator for OpenSearch](https://github.com/stackabletech/opensearch-operator)
* [Stackable Operator for Apache Spark](https://github.com/stackabletech/spark-k8s-operator)
* [Stackable Operator for Apache Superset](https://github.com/stackabletech/superset-operator)
* [Stackable Operator for Trino](https://github.com/stackabletech/trino-operator)
* [Stackable Operator for Apache ZooKeeper](https://github.com/stackabletech/zookeeper-operator)

And the operators they all build on:

* [Stackable Commons Operator](https://github.com/stackabletech/commons-operator)
* [Stackable Listener Operator](https://github.com/stackabletech/listener-operator)
* [Stackable Secret Operator](https://github.com/stackabletech/secret-operator)

## Contributing

Contributions are welcome.
Follow our [Contributors Guide](https://docs.stackable.tech/home/stable/contributor/index.html) to learn how you can contribute.
All contributors will have to sign a [Contributor License Agreement](https://github.com/stackabletech/.github/blob/main/cla.md).
This is enforced automatically when you submit a Pull Request where a bot will guide you through the process.

## License

[Open Software License version 3.0](./LICENSE).

## Support

Get started with the community edition! If you want professional support, [we offer subscription plans and custom licensing](https://stackable.tech/en/plans/).

## Sponsor

If you want to support our work but don't need professional support please consider [sponsoring](https://github.com/sponsors/stackabletech) our work.

<!-- markdownlint-disable MD041 MD022 MD032 -->
