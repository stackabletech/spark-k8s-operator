# Examples

## Overview

This note outlines a few things that are needed to run these examples on a local Kubernetes cluster.

## Cluster

Create a new local cluster (e.g. with [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/) and the [stackablectl tool](https://github.com/stackabletech/stackablectl)). This creates a cluster named `stackable-data-platform`.

````text
kind delete clusters --all
stackablectl operator install spark-k8s -k
````

Build the `ny-tlc-report` image from the Dockerfile in this repository (apps/docker/Dockerfile) and then load it to the cluster:

````text
kind load docker-image docker.stackable.tech/stackable/ny-tlc-report:0.1.0 --name stackable-data-platform
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
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set buckets[0].name=my-bucket,buckets[0].policy=public \
--set resources.requests.memory=1Gi \
--repo https://charts.min.io/ minio
````

````text
kubectl apply -f kind/minio.yaml
````

Several resources are needed in this store. These can be loaded like this:

````text
kubectl exec minio-mc-0 -- sh -c 'mc alias set test-minio http://test-minio:9000/'
kubectl cp examples/ny-tlc-report-1.1.0.jar  minio-mc-0:/tmp
kubectl cp examples/ny-tlc-report.py  minio-mc-0:/tmp
kubectl cp examples/yellow_tripdata_2021-07.csv  minio-mc-0:/tmp
kubectl exec minio-mc-0 -- mc cp /tmp/ny-tlc-report-1.1.0.jar test-minio/my-bucket
kubectl exec minio-mc-0 -- mc cp /tmp/ny-tlc-report.py test-minio/my-bucket
kubectl exec minio-mc-0 -- mc cp /tmp/yellow_tripdata_2021-07.csv test-minio/my-bucket
````

We now have a local S3-implementation with the bucket populated with the resources we need for the examples, which can be run like this:

````text
kubectl apply -f examples/ny-tlc-report-configmap.yaml
````