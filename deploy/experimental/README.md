# OLM package

## Usage

Prerequisite is of course a running OpenShift cluster.

First, install the operator using OLM:

    kubectl apply -f catalog-source.yaml \
    -f operator-group.yaml \
    -f subscription.yaml

Then, install the operator dependencies with Helm:

    helm install secret-operator stackable/secret-operator
    helm install commons-operator stackable/commons-operator

## Bilding bundle images

    cd deploy/experimental/olm

    rm -rf bundle
    mkdir bundle

    opm alpha bundle generate --directory manifests --package spark-k8s-operator --output-dir bundle --channels stable --default stable
    docker build -t docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest -f bundle.Dockerfile .
    docker push docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest
    opm alpha bundle validate --tag docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest --image-builder docker

    rm -rf catalog
    mkdir catalog

    opm generate dockerfile catalog
    opm init spark-k8s-operator --default-channel=preview --description=./README.md --output yaml > catalog/operator.yaml

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

## Notes on OLM bundles

* Specifying custom SCCs is not supported. Need to use pre-existing SCCs.

        ➜  olm git:(olm-packaging) ✗ opm alpha bundle validate --tag docker.stackable.tech/stackable/spark-k8s-operator-bundle:latest --image-builder docker
        INFO[0000] Create a temp directory at /tmp/bundle-1553779108  container-tool=docker
        DEBU[0000] Pulling and unpacking container image         container-tool=docker
        ...
        DEBU[0000] Validating "security.openshift.io/v1, Kind=SecurityContextConstraints" from file "scc.yaml"  container-tool=docker
        DEBU[0000] Validating "apiextensions.k8s.io/v1, Kind=CustomResourceDefinition" from file "sparkapplication.crd.yaml"  container-tool=docker
        Error: Bundle validation errors: SecurityContextConstraints is not supported type for registryV1 bundle: /tmp/bundle-1553779108/manifests/scc.yaml
