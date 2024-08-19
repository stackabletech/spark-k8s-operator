---
name: New Version
about: Request support for a new product version
title: "[NEW VERSION]"
labels: ''
assignees: ''

---

## Which new version of Apache Spark-on-Kubernetes should we support?

Please specify the version, version range or version numbers to support, please also add these to the issue title

## Additional information

If possible, provide a link to release notes/changelog

## Changes required

Are there any upstream changes that we need to support?
e.g. new features, changed features, deprecated features etc.

## Implementation checklist

<!--
    Please don't change anything in this list.
    Not all of these steps are necessary for all versions.
-->

- [ ] Update the Docker image
- [ ] Update documentation to include supported version(s)
- [ ] Update and test getting started guide with updated version(s)
- [ ] Update operator to support the new version (if needed)
- [ ] Update integration tests to test use the new versions (in addition or replacing old versions
- [ ] Update examples to use new versions
