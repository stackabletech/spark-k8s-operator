#!/usr/bin/env sh

rm -rf bundle
mkdir bundle

opm alpha bundle generate --directory manifests --package spark-k8s-operator --output-dir bundle --channels stable --default stable
docker build -t docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest -f bundle.Dockerfile .
docker push docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest
opm alpha bundle validate --tag docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest --image-builder docker

rm -rf catalog
rm -f catalog.Dockerfile
mkdir catalog

opm generate dockerfile catalog
opm init spark-k8s-operator --default-channel=preview --description=../README.md --output yaml > catalog/operator.yaml

cat << EOF >> catalog/operator.yaml
---
schema: olm.channel
package: spark-k8s-operator
name: preview
entries:
- name: spark-k8s-operator
EOF

opm render docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest --output=yaml >> catalog/operator.yaml
opm validate catalog

docker build .  -f catalog.Dockerfile -t docker.stackable.tech/stackable/spark-k8s-operator-catalog:latest
docker push docker.stackable.tech/stackable/spark-k8s-operator-catalog:latest
