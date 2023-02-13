#!/usr/bin/env bash
set -euo pipefail

# This script contains all the code snippets from the guide, as well as some assert tests
# to test if the instructions in the guide work. The user *could* use it, but it is intended
# for testing only.
# The script will install the operators, create a superset instance and briefly open a port
# forward and connect to the superset instance to make sure it is up and running.
# No running processes are left behind (i.e. the port-forwarding is closed at the end)

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

case "$1" in
"helm")
echo "Adding 'stackable-stable' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-stable https://repo.stackable.tech/repository/helm-stable/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-stable/commons-operator --version 23.1.0
helm install --wait secret-operator stackable-stable/secret-operator --version 23.1.0
helm install --wait spark-k8s-operator stackable-stable/spark-k8s-operator --version 23.1.0
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=23.1.0 \
  secret=23.1.0 \
  spark-k8s=23.1.0
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating a Spark Application..."
# tag::install-sparkapp[]
kubectl apply -f - <<EOF
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: pyspark-pi
  namespace: default
spec:
  version: "1.0"
  sparkImage: docker.stackable.tech/stackable/pyspark-k8s:3.3.0-stackable0.3.0
  mode: cluster
  mainApplicationFile: local:///stackable/spark/examples/src/main/python/pi.py
  driver:
    resources:
      cpu:
        min: "1"
        max: "1200m"
      memory:
        limit: "1Gi"
  executor:
    instances: 3
    resources:
      cpu:
        min: "1"
        max: "1200m"
      memory:
        limit: "1Gi"
EOF
# end::install-sparkapp[]

echo "Waiting for job to complete ..."
# tag::wait-for-job[]
kubectl wait pods -l 'job-name=pyspark-pi' \
  --for jsonpath='{.status.phase}'=Succeeded \
  --timeout 300s
# end::wait-for-job[]

result=$(kubectl logs -l 'spark-role=driver' --tail=-1 | grep "Pi is roughly")

if [ "$result" == "" ]; then
  echo "Log result was not found!"
  exit 1
else
  echo "Job result:" "$result"
fi
