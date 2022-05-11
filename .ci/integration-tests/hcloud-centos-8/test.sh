#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/spark-k8s-operator.git
(cd spark-k8s-operator/ && ./scripts/run_tests.sh --parallel 1)
exit_code=$?
./operator-logs.sh spark-k8s > /target/spark-k8s-operator.log
./operator-logs.sh spark > /target/spark-operator.log
exit $exit_code
