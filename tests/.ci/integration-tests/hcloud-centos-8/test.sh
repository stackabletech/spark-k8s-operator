git clone -b $GIT_BRANCH https://github.com/stackabletech/integration-tests.git
(cd integration-tests/spark-k8s-operator && kubectl kuttl test)
exit_code=$?
./operator-logs.sh spark-k8s > /target/spark-k8s-operator.log
exit $exit_code
