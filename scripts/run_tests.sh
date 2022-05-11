#!/usr/bin/env bash
set -e

# Register absolute paths to pass to Ansible so the location of the role is irrelevant
# for the run
TESTDIR="$(pwd)/tests"
WORKDIR="$(pwd)/tests/_work"

# Create dirs
mkdir -p tests/ansible/roles
mkdir -p "$WORKDIR"

# Install Ansible role if needed
pushd tests/ansible
ansible-galaxy role install -r requirements.yaml -p ./roles

# TODO: create pipenv in files for script thingy

# Funnel via JSON to ensure that values are escaped properly
echo '{}' | jq '{work_dir: $WORKDIR, test_dir: $TESTDIR}' --arg WORKDIR "$WORKDIR" --arg TESTDIR "$TESTDIR" > "${WORKDIR}"/vars.json

# Run playbook to generate test scenarios
ansible-playbook playbook.yaml --extra-vars "@${WORKDIR}/vars.json"
popd

# Run tests
pushd tests/_work
kubectl kuttl test "$@"
popd
