## Check and Update Getting Started Script

<!--
    Make sure to update the link in 'issues/.github/ISSUE_TEMPLATE/pre-release-getting-started-scripts.md'
    when you rename this file.
-->

<!--
    Replace 'TRACKING_ISSUE' with the applicable release tracking issue number.
-->

Part of <https://github.com/stackabletech/issues/issues/TRACKING_ISSUE>

> [!NOTE]
> During a Stackable release we need to check (and optionally update) the
> getting-started scripts to ensure they still work after product and operator
> updates.

```shell
# Some of the scripts are in a code/ subdirectory
# pushd docs/modules/superset/examples/getting_started
# pushd docs/modules/superset/examples/getting_started/code
pushd $(fd -td getting_started | grep examples); cd code 2>/dev/null || true

# Make a fresh cluster (~12 seconds)
kind delete cluster && kind create cluster
./getting_started.sh stackablectl

# Make a fresh cluster (~12 seconds)
kind delete cluster && kind create cluster
./getting_started.sh helm

popd
```
