# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Updated stackable image versions ([#176])
- `operator-rs` `0.22.0` → `0.27.1` ([#178])
- Don't run init container as root and avoid chmod and chowning ([#183])

[#176]: https://github.com/stackabletech/spark-k8s-operator/pull/176
[#178]: https://github.com/stackabletech/spark-k8s-operator/pull/178
[#183]: https://github.com/stackabletech/spark-k8s-operator/pull/183

## [0.6.0] - 2022-11-07

### Changed

- Bumped image to `3.3.0-stackable0.2.0` in tests and docs ([#145])
- BREAKING: use resource limit struct instead of passing spark configuration arguments ([#147])
- Fixed resources test ([#151])
- Fixed inconsistencies with resources usage ([#166])

[#145]: https://github.com/stackabletech/spark-k8s-operator/pull/145
[#147]: https://github.com/stackabletech/spark-k8s-operator/pull/147
[#151]: https://github.com/stackabletech/spark-k8s-operator/pull/151
[#166]: https://github.com/stackabletech/spark-k8s-operator/pull/166

## [0.5.0] - 2022-09-06

### Added

- Add Getting Started documentation ([#114]).

[#114]: https://github.com/stackabletech/spark-k8s-operator/pull/114

### Fixed

- Add missing role to read S3Connection and S3Bucket objects ([#112]).
- Update annotation due to update to rust version ([#114]).
- Update RBAC properties for OpenShift compatibility ([#126]).

[#112]: https://github.com/stackabletech/spark-k8s-operator/pull/112
[#114]: https://github.com/stackabletech/spark-k8s-operator/pull/114
[#126]: https://github.com/stackabletech/spark-k8s-operator/pull/126

## [0.4.0] - 2022-08-03

### Changed

- Include chart name when installing with a custom release name ([#97])
- Pinned MinIO version for tests ([#100])
- `operator-rs` `0.21.0` → `0.22.0` ([#102]).
- Added owner-reference to pod templates ([#104])
- Added kuttl test for the case when pyspark jobs are provisioned using the `image` property of the `SparkApplication` definition ([#107])

[#97]: https://github.com/stackabletech/spark-k8s-operator/pull/92
[#100]: https://github.com/stackabletech/spark-k8s-operator/pull/100
[#102]: https://github.com/stackabletech/spark-k8s-operator/pull/102
[#104]: https://github.com/stackabletech/spark-k8s-operator/pull/104
[#107]: https://github.com/stackabletech/spark-k8s-operator/pull/107

## [0.3.0] - 2022-06-30

### Added

### Changed

- BREAKING: Use current S3 connection/bucket structs ([#86])
- Add node selector to top-level job and specify node selection in PVC-relevant tests ([#90])
- Update kuttl tests to use Spark 3.3.0 ([#91])
- Bugfix for duplicate volume mounts in PySpark jobs ([#92])

[#86]: https://github.com/stackabletech/spark-k8s-operator/pull/86
[#90]: https://github.com/stackabletech/spark-k8s-operator/pull/90
[#91]: https://github.com/stackabletech/spark-k8s-operator/pull/91
[#92]: https://github.com/stackabletech/spark-k8s-operator/pull/92

## [0.2.0] - 2022-06-21

### Added

- Added new fields to govern image pull policy ([#75])
- New `nodeSelector` fields for both the driver and the executors ([#76])
- Mirror driver pod status to the corresponding spark application ([#77])

[#75]: https://github.com/stackabletech/spark-k8s-operator/pull/75
[#76]: https://github.com/stackabletech/spark-k8s-operator/pull/76
[#77]: https://github.com/stackabletech/spark-k8s-operator/pull/77

### Changed

- Updated examples ([#71])

[#71]: https://github.com/stackabletech/spark-k8s-operator/pull/71

## [0.1.0] - 2022-05-05

### Added

- Initial commit
- ServiceAccount, ClusterRole and RoleBinding for Spark driver ([#39])
- S3 credentials can be provided via a Secret ([#42])
- Job information can be passed via a configuration map ([#50])
- Update S3 bucket specification to be conform with the corresponding ADR ([#55])

[#39]: https://github.com/stackabletech/spark-k8s-operator/pull/39
[#42]: https://github.com/stackabletech/spark-k8s-operator/pull/42
[#50]: https://github.com/stackabletech/spark-k8s-operator/pull/50
[#55]: https://github.com/stackabletech/spark-k8s-operator/pull/55
