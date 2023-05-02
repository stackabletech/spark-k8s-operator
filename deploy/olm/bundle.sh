#!/usr/bin/env bash
# usage: bundle.sh <release>, called from base folder:
# e.g. ./deploy/olm/bundle.sh 23.1.0

set -euo pipefail
set -x

OPERATOR_NAME="spark-operator"

bundle-clean() {
	rm -rf "deploy/olm/${VERSION}/bundle"
	rm -rf "deploy/olm/${VERSION}/bundle.Dockerfile"
}


build-bundle() {
	opm alpha bundle generate --directory manifests --package "${OPERATOR_NAME}-package" --output-dir bundle --channels stable --default stable
  cp metadata/*.yaml bundle/metadata/
  docker build -t "docker.stackable.tech/stackable/${OPERATOR_NAME}-bundle:${VERSION}" -f bundle.Dockerfile .
  docker push "docker.stackable.tech/stackable/${OPERATOR_NAME}-bundle:${VERSION}"
  opm alpha bundle validate --tag "docker.stackable.tech/stackable/${OPERATOR_NAME}-bundle:${VERSION}" --image-builder docker
}

main() {
  VERSION="$1";

  pushd "deploy/olm/${VERSION}"
  bundle-clean
  build-bundle
  popd
}

main "$@"