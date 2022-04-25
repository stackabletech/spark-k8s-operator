#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/spark-k8s-operator.git
(cd spark-k8s-operator/ && ./scripts/run_tests.sh)
exit_code=$?
./operator-logs.sh spark-k8s > /target/spark-k8s-operator.log
exit $exit_code