# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Experimental support for Spark Connect ([#539]).
- Adds new telemetry CLI arguments and environment variables ([#560]).
  - Use `--file-log-max-files` (or `FILE_LOG_MAX_FILES`) to limit the number of log files kept.
  - Use `--file-log-rotation-period` (or `FILE_LOG_ROTATION_PERIOD`) to configure the frequency of rotation.
  - Use `--console-log-format` (or `CONSOLE_LOG_FORMAT`) to set the format to `plain` (default) or `json`.
- Expose history and connect services via listener classes ([#562]).
- Support for Spark 3.5.6 ([#580]).

### Changed

- BREAKING: Replace stackable-operator `initialize_logging` with stackable-telemetry `Tracing` ([#547], [#554], [#560]).
  - The console log level was set by `SPARK_K8S_OPERATOR_LOG`, and is now set by `CONSOLE_LOG_LEVEL`.
  - The file log level was set by `SPARK_K8S_OPERATOR_LOG`, and is now set by `FILE_LOG_LEVEL`.
  - The file log directory was set by `SPARK_K8S_OPERATOR_LOG_DIRECTORY`, and is now set
    by `FILE_LOG_DIRECTORY` (or via `--file-log-directory <DIRECTORY>`).
  - Replace stackable-operator `print_startup_string` with `tracing::info!` with fields.
- BREAKING: Inject the vector aggregator address into the vector config using the env var `VECTOR_AGGREGATOR_ADDRESS` instead
  of having the operator write it to the vector config ([#551]).
- Document that Spark Connect doesn't integrate with the history server ([#559])
- test: Bump to Vector `0.46.1` ([#565]).
- Use versioned common structs ([#572]).
- BREAKING: Change the label `app.kubernetes.io/name` for Spark history and connect objects to use `spark-history` and `spark-connect` instead of `spark-k8s` ([#573]).
- BREAKING: The history Pods now have their own ClusterRole named `spark-history-clusterrole` ([#573]).
- BREAKING: Previously this operator would hardcode the UID and GID of the Pods being created to 1000/0, this has changed now ([#575])
  - The `runAsUser` and `runAsGroup` fields will not be set anymore by the operator
  - The defaults from the docker images itself will now apply, which will be different from 1000/0 going forward
  - This is marked as breaking because tools and policies might exist, which require these fields to be set
- Enable the built-in Prometheus servlet. The jmx exporter was removed in ([#584]) but added back in ([#585]).

### Fixed

- Use `json` file extension for log files ([#553]).
- The Spark connect controller now watches StatefulSets instead of Deployments (again) ([#573]).

### Removed

- Support for Spark versions 3.5.2 has been dropped ([#570]).
- Integration test spark-pi-public-s3 because the AWS SDK >2.24 doesn't suuport anonymous S3 access anymore ([#574]).

[#539]: https://github.com/stackabletech/spark-k8s-operator/pull/539
[#547]: https://github.com/stackabletech/spark-k8s-operator/pull/547
[#551]: https://github.com/stackabletech/spark-k8s-operator/pull/551
[#553]: https://github.com/stackabletech/spark-k8s-operator/pull/553
[#554]: https://github.com/stackabletech/spark-k8s-operator/pull/554
[#559]: https://github.com/stackabletech/spark-k8s-operator/pull/559
[#560]: https://github.com/stackabletech/spark-k8s-operator/pull/560
[#562]: https://github.com/stackabletech/spark-k8s-operator/pull/562
[#565]: https://github.com/stackabletech/spark-k8s-operator/pull/565
[#570]: https://github.com/stackabletech/spark-k8s-operator/pull/570
[#572]: https://github.com/stackabletech/spark-k8s-operator/pull/572
[#573]: https://github.com/stackabletech/spark-k8s-operator/pull/573
[#574]: https://github.com/stackabletech/spark-k8s-operator/pull/574
[#580]: https://github.com/stackabletech/spark-k8s-operator/pull/580
[#575]: https://github.com/stackabletech/spark-k8s-operator/pull/575
[#584]: https://github.com/stackabletech/spark-k8s-operator/pull/584
[#585]: https://github.com/stackabletech/spark-k8s-operator/pull/585

## [25.3.0] - 2025-03-21

### Added

- The lifetime of auto generated TLS certificates is now configurable with the role and roleGroup
  config property `requestedSecretLifetime`. This helps reducing frequent Pod restarts ([#501]).
- Run a `containerdebug` process in the background of each Spark container to collect debugging information ([#508]).
- Aggregate emitted Kubernetes events on the CustomResources ([#515]).
- Support configuring JVM arguments ([#532]).
- Support for S3 region ([#528]).

### Changed

- Default to OCI for image metadata and product image selection ([#514]).
- Update tests and docs to Spark version 3.5.5 ([#534])

[#501]: https://github.com/stackabletech/spark-k8s-operator/pull/501
[#508]: https://github.com/stackabletech/spark-k8s-operator/pull/508
[#514]: https://github.com/stackabletech/spark-k8s-operator/pull/514
[#515]: https://github.com/stackabletech/spark-k8s-operator/pull/515
[#528]: https://github.com/stackabletech/spark-k8s-operator/pull/528
[#532]: https://github.com/stackabletech/spark-k8s-operator/pull/532
[#534]: https://github.com/stackabletech/spark-k8s-operator/pull/534

## [24.11.1] - 2025-01-10

## [24.11.0] - 2024-11-18

### Added

- Make spark-env.sh configurable via `configOverrides` ([#473]).
- The Spark history server can now service logs from HDFS compatible systems ([#479]).
- The operator can now run on Kubernetes clusters using a non-default cluster domain.
  Use the env var `KUBERNETES_CLUSTER_DOMAIN` or the operator Helm chart property `kubernetesClusterDomain` to set a non-default cluster domain ([#480]).

### Changed

- Reduce CRD size from `1.2MB` to `103KB` by accepting arbitrary YAML input instead of the underlying schema for the following fields ([#450]):
  - `podOverrides`
  - `affinity`
  - `volumes`
  - `volumeMounts`
- Update tests and docs to Spark version 3.5.2 ([#459])

### Fixed

- BREAKING: The fields `connection` and `host` on `S3Connection` as well as `bucketName` on `S3Bucket`are now mandatory ([#472]).
- Fix `envOverrides` for SparkApplication and SparkHistoryServer ([#451]).
- Ensure SparkApplications can only create a single submit Job. Fix for #457 ([#460]).
- Invalid `SparkApplication`/`SparkHistoryServer` objects don't cause the operator to stop functioning (#[482]).

### Removed

- Support for Spark versions 3.4.2 and 3.4.3 has been dropped ([#459]).

[#450]: https://github.com/stackabletech/spark-k8s-operator/pull/450
[#451]: https://github.com/stackabletech/spark-k8s-operator/pull/451
[#459]: https://github.com/stackabletech/spark-k8s-operator/pull/459
[#460]: https://github.com/stackabletech/spark-k8s-operator/pull/460
[#472]: https://github.com/stackabletech/spark-k8s-operator/pull/472
[#473]: https://github.com/stackabletech/spark-k8s-operator/pull/473
[#479]: https://github.com/stackabletech/spark-k8s-operator/pull/479
[#480]: https://github.com/stackabletech/spark-k8s-operator/pull/480

## [24.7.0] - 2024-07-24

### Changed

- Bump `stackable-operator` to 0.70.0, `product-config` to 0.7.0, and other dependencies ([#401], [#425]).

### Fixed

- BREAKING (behaviour): Specified CPU resources are now applied correctly (instead of rounding it to the next whole number).
  This might affect your jobs, as they now e.g. only have 200m CPU resources requested instead of the 1000m it had so far,
  meaning they might slow down significantly ([#408]).
- Processing of corrupted log events fixed; If errors occur, the error
  messages are added to the log event ([#412]).

[#401]: https://github.com/stackabletech/spark-k8s-operator/pull/401
[#408]: https://github.com/stackabletech/spark-k8s-operator/pull/408
[#412]: https://github.com/stackabletech/spark-k8s-operator/pull/412
[#425]: https://github.com/stackabletech/spark-k8s-operator/pull/425

## [24.3.0] - 2024-03-20

### Added

- Helm: support labels in values.yaml ([#344]).
- Support version `3.5.1` ([#373]).
- Support version `3.4.2` ([#357]).
- `spec.job.config.volumeMounts` property to easily mount volumes on the job pod ([#359])

### Changed

- Various documentation of the CRD ([#319]).
- [BREAKING] Removed version field. Several attributes have been changed to mandatory. While this change is
  technically breaking, existing Spark jobs would not have worked before as these attributes were necessary ([#319]).
- [BREAKING] Remove `userClassPathFirst` properties from `spark-submit`. This is an experimental feature that was
  introduced to support logging in XML format. The side effect of this removal is that the vector agent cannot
  aggregate output from the `spark-submit` containers. On the other side, it enables dynamic provisionining of
  java packages (such as Delta Lake) with Stackable stock images which is much more important. ([#355])

### Fixed

- Add missing `deletecollection` RBAC permission for Spark drivers. Previously this caused confusing error
  messages in the spark driver log (`User "system:serviceaccount:default:my-spark-app" cannot deletecollection resource "configmaps" in API group "" in the namespace "default".`) ([#313]).

[#313]: https://github.com/stackabletech/spark-k8s-operator/pull/313
[#319]: https://github.com/stackabletech/spark-k8s-operator/pull/319
[#344]: https://github.com/stackabletech/spark-k8s-operator/pull/344
[#355]: https://github.com/stackabletech/spark-k8s-operator/pull/355
[#357]: https://github.com/stackabletech/spark-k8s-operator/pull/357
[#359]: https://github.com/stackabletech/spark-k8s-operator/pull/359
[#373]: https://github.com/stackabletech/spark-k8s-operator/pull/373

## [23.11.0] - 2023-11-24

### Added

- Default stackableVersion to operator version. It is recommended to remove `spec.image.stackableVersion` from your custom resources ([#267], [#268]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#272]).
- Support PodDisruptionBudgets for HistoryServer ([#288]).
- Support for versions 3.4.1, 3.5.0 ([#291]).
- History server now exports metrics via jmx exporter (port 18081) ([#291]).
- Document graceful shutdown ([#306]).

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
- Re-instated driver/executor cores setting ([#302]).

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
[#302]: https://github.com/stackabletech/spark-k8s-operator/pull/302
[#306]: https://github.com/stackabletech/spark-k8s-operator/pull/306

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
