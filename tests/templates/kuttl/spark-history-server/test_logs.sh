#!/usr/bin/env bash

result=$(mc find test-minio/spark-logs/eventlogs --name "spark-*" | wc -l)
# expected: 2
echo "Logged jobs: $result"

if [ "$result" == '2' ]; then
  echo "[SUCCESS] History server logs test was successful!"
else
  echo "[ERROR] 2 jobs were expected!"
  exit 1
fi
