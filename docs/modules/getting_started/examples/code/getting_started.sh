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
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-dev/commons-operator --version 0.3.0-nightly
helm install --wait secret-operator stackable-dev/secret-operator --version 0.6.0-nightly
helm install --wait spark-k8s-operator stackable-dev/spark-k8s-operator --version 0.5.0-nightly
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.3.0-nightly \
  secret=0.6.0-nightly \
  spark-k8s=0.5.0-nightly
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Creating a Spark Application"
# tag::install-sparkapp[]
kubectl apply -f pyspark-pi.yaml
# end::install-sparkapp[]

echo "Waiting on AirflowDB ..."
# tag::wait-airflowdb[]
kubectl wait airflowdb/airflow \
  --for jsonpath='{.status.condition}'=Ready \
  --timeout 300s
# end::wait-airflowdb[]

sleep 5

echo "Awaiting Airflow rollout finish"
# tag::watch-airflow-rollout[]
kubectl rollout status --watch statefulset/airflow-webserver-default
kubectl rollout status --watch statefulset/airflow-worker-default
kubectl rollout status --watch statefulset/airflow-scheduler-default
# end::watch-airflow-rollout[]

echo "Starting port-forwarding of port 8080"
# tag::port-forwarding[]
kubectl port-forward svc/airflow-webserver 8080 2>&1 >/dev/null &
# end::port-forwarding[]
PORT_FORWARD_PID=$!
trap "kill $PORT_FORWARD_PID" EXIT
sleep 5

echo "Checking if web interface is reachable ..."
return_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/login/)
if [ "$return_code" == 200 ]; then
  echo "Webserver UI reachable!"
else
  echo "Could not reach Webserver UI."
  exit 1
fi
