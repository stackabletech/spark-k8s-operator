#!/usr/bin/env bash
set -euo pipefail

# DO NOT EDIT THE SCRIPT
# Instead, update the j2 template, and regenerate it for dev with `make render-docs`.

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

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 25.11.0
helm install --wait secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 25.11.0
helm install --wait listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 25.11.0
helm install --wait spark-k8s-operator oci://oci.stackable.tech/sdp-charts/spark-k8s-operator --version 25.11.0
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=25.11.0 \
  secret=25.11.0 \
  listener=25.11.0 \
  spark-k8s=25.11.0
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating a Spark Application..."
# tag::install-sparkapp[]
kubectl apply -f application.yaml
# end::install-sparkapp[]

sleep 15

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
