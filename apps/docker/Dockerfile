# We want to automatically use the latest. We also don't tag our images with a version.
# hadolint ignore=DL3007
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest AS builder
LABEL maintainer="Stackable GmbH"

WORKDIR /jobs

# A user must be specified for the container to run on OpenShift
# with the nonroot-v2 security context constraint and
# without a runAsUser container annotation.
USER 1000

COPY apps/ny_tlc_report.py .
