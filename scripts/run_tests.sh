#!/usr/bin/env bash

mkdir -p ansible/roles

pushd ansible
ansible-galaxy role install -r requirements.yaml -p ./roles
# TODO: create pipenv in files for script thingy

# Write test scenarios
ansible-playbook playbook.yaml

popd
# Run tests
pushd test-work
kubectl kuttl test
