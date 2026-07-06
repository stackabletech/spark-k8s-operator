# product-config-compat fixtures

The fixtures in `fixtures/` are the expected baseline for operator-generated Spark objects
created by a getting-started style `SparkApplication` (`pyspark-pi`).

Compared objects:

- `pyspark-pi-driver-pod-template` ConfigMap `.data`
- `pyspark-pi-executor-pod-template` ConfigMap `.data`
- `pyspark-pi-submit-job` ConfigMap `.data`
- `pyspark-pi` Job `.spec.template.spec`
- `spark-history-node-default` ConfigMap `.data`
- `spark-connect-server` ConfigMap `.data`
- `spark-connect-executor` ConfigMap `.data`

## Refresh baseline fixtures

Refresh these files from the `main` branch when an intentional behavior change is accepted.

Example workflow:

1. Check out `main`.
2. Ensure the spark-k8s operator from `main` is running in your local cluster.
3. Apply a getting-started style `SparkApplication`, plus `SparkHistoryServer` and `SparkConnectServer`, in a temporary namespace.
4. Export the objects listed above and replace namespace-specific values with `__NAMESPACE__` in fixtures that embed it.
5. Commit fixture updates together with the intentional behavior change.
