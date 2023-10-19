# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Default stackableVersion to operator version. It is recommended to remove `spec.image.stackableVersion` from your custom resources ([#267], [#268]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#272]).
- Support PodDisruptionBudgets for HistoryServer ([#288]).
- Support for versions 3.4.1, 3.5.0 ([#291]).
- History server now exports metrics via jmx exporter (port 18081) ([#291]).

### Changed

- `vector` `0.26.0` -> `0.33.0` ([#269], [#291]).
- `operator-rs` `0.44.0` -> `0.55.0` ([#267], [#275], [#288], [#291]).
- Removed usages of SPARK_DAEMON_JAVA_OPTS since it's not a reliable way to pass extra JVM options ([#272]).
- [BREAKING] use product image selection instead of version ([#275]).
- [BREAKING] refactored application roles to use `CommonConfiguration` structures from the operator framework ([#277]).
- Let secret-operator handle certificate conversion ([#286]).
- Extended resource-usage documentation ([#297]).

### Fixed

- Dynamic loading of Maven packages ([#281]).

### Removed

- Removed support for versions 3.2.1, 3.3.0 ([#291]).

[#267]: https://github.com/stackabletech/spark-k8s-operator/pull/267
[#268]: https://github.com/stackabletech/spark-k8s-operator/pull/268
[#269]: https://github.com/stackabletech/spark-k8s-operator/pull/269
[#272]: https://github.com/stackabletech/spark-k8s-operator/pull/272
[#275]: https://github.com/stackabletech/spark-k8s-operator/pull/275
[#277]: https://github.com/stackabletech/spark-k8s-operator/pull/277
[#281]: https://github.com/stackabletech/spark-k8s-operator/pull/281
[#286]: https://github.com/stackabletech/spark-k8s-operator/pull/286
[#288]: https://github.com/stackabletech/spark-k8s-operator/pull/288
[#291]: https://github.com/stackabletech/spark-k8s-operator/pull/291
[#297]: https://github.com/stackabletech/spark-k8s-operator/pull/297

## [23.7.0] - 2023-07-14

### Added

- Generate OLM bundle for Release 23.4.0 ([#238]).
- Add support for Spark 3.4.0 ([#243]).
- Add support for using custom certificates when accessing S3 with TLS ([#247]).
- Use bitnami charts for testing S3 access with TLS ([#247]).
- Set explicit resources on all containers ([#249]).
- Support pod overrides ([#256]).

### Changed

- `operator-rs` `0.38.0` -> `0.44.0` ([#235], [#259]).
- Use 0.0.0-dev product images for testing ([#236]).
- Use testing-tools 0.2.0 ([#236]).
- Run as root group ([#241]).
- Added kuttl test suites ([#252]).

### Fixed

- Fix quoting issues when spark config values contain spaces ([#243]).
- Increase the size limit of log volumes ([#259]).
- Typo in executor cpu limit property ([#263]).

[#235]: https://github.com/stackabletech/spark-k8s-operator/pull/235
[#236]: https://github.com/stackabletech/spark-k8s-operator/pull/236
[#238]: https://github.com/stackabletech/spark-k8s-operator/pull/238
[#241]: https://github.com/stackabletech/spark-k8s-operator/pull/241
[#243]: https://github.com/stackabletech/spark-k8s-operator/pull/243
[#247]: https://github.com/stackabletech/spark-k8s-operator/pull/247
[#252]: https://github.com/stackabletech/spark-k8s-operator/pull/252
[#249]: https://github.com/stackabletech/spark-k8s-operator/pull/249
[#256]: https://github.com/stackabletech/spark-k8s-operator/pull/256
[#259]: https://github.com/stackabletech/spark-k8s-operator/pull/259
[#263]: https://github.com/stackabletech/spark-k8s-operator/pull/263

## [23.4.0] - 2023-04-17

### Added

- Deploy default and support custom affinities ([#217])
- Log aggregation added ([#226]).

### Changed

- [BREAKING] Support specifying Service type for HistoryServer.
  This enables us to later switch non-breaking to using `ListenerClasses` for the exposure of Services.
  This change is breaking, because - for security reasons - we default to the `cluster-internal` `ListenerClass`.
  If you need your cluster to be accessible from outside of Kubernetes you need to set `clusterConfig.listenerClass`
  to `external-unstable` or `external-stable` ([#228]).
- [BREAKING]: Dropped support for old `spec.{driver,executor}.nodeSelector` field. Use `spec.{driver,executor}.affinity.nodeSelector` instead ([#217])
- Revert openshift settings ([#207])
- BUGFIX: assign service account to history pods ([#207])
- Merging and validation of the configuration refactored ([#223])
- `operator-rs` `0.36.0` → `0.38.0` ([#223])

[#207]: https://github.com/stackabletech/spark-k8s-operator/pull/207
[#217]: https://github.com/stackabletech/spark-k8s-operator/pull/217
[#223]: https://github.com/stackabletech/spark-k8s-operator/pull/223
[#226]: https://github.com/stackabletech/spark-k8s-operator/pull/226
[#228]: https://github.com/stackabletech/spark-k8s-operator/pull/228

## [23.1.0] - 2023-01-23

### Added

- Create and manage history servers ([#187])

[#187]: https://github.com/stackabletech/spark-k8s-operator/pull/187

### Changed

- Updated stackable image versions ([#176])
- `operator-rs` `0.22.0` → `0.27.1` ([#178])
- `operator-rs` `0.27.1` -> `0.30.2` ([#187])
- Don't run init container as root and avoid chmod and chowning ([#183])
- [BREAKING] Implement fix for S3 reference inconsistency as described in the issue #162 ([#187])

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
