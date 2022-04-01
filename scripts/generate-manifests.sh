#!/usr/bin/env bash
# This script reads a Helm chart from deploy/helm/spark-k8s-operator and
# generates manifest files into deploy/manifestss
set -e

tmp=$(mktemp -d ./manifests-XXXXX)

helm template --output-dir "$tmp" \
              --include-crds \
              --name-template spark-k8s-operator \
              deploy/helm/spark-k8s-operator

for file in $tmp/spark-k8s-operator/*/*; do
  yq eval -i 'del(.. | select(has("app.kubernetes.io/managed-by")) | ."app.kubernetes.io/managed-by")' /dev/stdin < "$file"
  yq eval -i 'del(.. | select(has("helm.sh/chart")) | ."helm.sh/chart")' /dev/stdin < "$file"
  sed -i '/# Source: .*/d' "$file"
done

cp -r "$tmp"/spark-k8s-operator/*/* deploy/manifests/

rm -rf "$tmp"
