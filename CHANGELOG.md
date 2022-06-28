# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

### Changed

- BREAKING: Use current S3 connection/bucket structs ([#86])
- Add node selector to top-level job and specify node selection in PVC-relevant tests ([#90])

[#86]: https://github.com/stackabletech/spark-k8s-operator/pull/86
[#90]: https://github.com/stackabletech/spark-k8s-operator/pull/90

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
