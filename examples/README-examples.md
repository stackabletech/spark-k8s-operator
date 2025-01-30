# Examples

## Overview

This note outlines a few things that are needed to run these examples on a local Kubernetes cluster.

## Cluster

Create a new local cluster (e.g. with [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/) and the [stackablectl tool](https://github.com/stackabletech/stackablectl)). This creates a cluster named `stackable-data-platform`.

````text
kind create cluster --name stackable-data-platform
stackablectl operator install spark-k8s commons secret
````

Load the `ny-tlc-report` image to the cluster:

````text
kind load docker-image oci.stackable.tech/stackable/ny-tlc-report:0.2.0 --name stackable-data-platform
````

## Set up the `PersistentVolumeClaim`

The PVC should contain a few dependencies that Spark needs to access S3:

````text
kubectl apply -f kind/kind-pvc.yaml
````

## Set up the `minio` object store

Use a local object store to avoid external dependencies:

````text
helm install test-minio \
--version 4.0.15 \
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set 'buckets[0].name=my-bucket,buckets[0].policy=public' \
--set resources.requests.memory=1Gi \
--repo https://charts.min.io/ minio
````

````text
kubectl apply -f kind/minio.yaml
````

Several resources are needed in this store. These can be loaded like this:

````text
kubectl exec minio-mc-0 -- sh -c 'mc alias set test-minio http://test-minio:9000/'
kubectl cp examples/ny-tlc-report-1.1.0-3.5.2.jar  minio-mc-0:/tmp
kubectl cp apps/ny_tlc_report.py  minio-mc-0:/tmp
kubectl cp examples/yellow_tripdata_2021-07.csv  minio-mc-0:/tmp
kubectl exec minio-mc-0 -- mc cp /tmp/ny-tlc-report-1.1.0-3.5.2.jar test-minio/my-bucket
kubectl exec minio-mc-0 -- mc cp /tmp/ny_tlc_report.py test-minio/my-bucket
kubectl exec minio-mc-0 -- mc cp /tmp/yellow_tripdata_2021-07.csv test-minio/my-bucket
````

We now have a local S3-implementation with the bucket populated with the resources we need for the examples, which can be run like this:

````text
kubectl apply -f examples/ny-tlc-report.yaml
````
