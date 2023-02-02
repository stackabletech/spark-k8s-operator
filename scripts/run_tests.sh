#!/usr/bin/env bash

# Check if the test expansion tool beku is installed
set +e
which beku > /dev/null 2>&1
beku_installed=$?
set -e
if [ $beku_installed -ne 0 ]; then
  echo "Please install beku.py to run the tests, see https://github.com/stackabletech/beku.py"
  exit 1
fi

echo "Using beku version: $(beku --version)"

# cleanup any old tests
rm -rf tests/_work

# Expand the tests
beku

# Run tests, pass the params
pushd tests/_work
kubectl kuttl test "$@"
popd
