## Bump Rust Dependencies for Stackable Release YY.M.X

<!--
    Make sure to update the link in 'issues/.github/ISSUE_TEMPLATE/pre-release-operator-rust-deps.md'
    when you rename this file.
-->

<!--
    Replace 'TRACKING_ISSUE' with the applicable release tracking issue number.
-->

Part of <https://github.com/stackabletech/issues/issues/TRACKING_ISSUE>

> [!NOTE]
> During a Stackable release we need to update various Rust dependencies before
> entering the final release period to ensure we run the latest versions of
> crates. These bumps also include previously updated and released crates from
> the `operator-rs` repository.

### Tasks

- [ ] Bump Rust Dependencies, see below for more details.
- [ ] Add changelog entry stating which important crates were bumped (including the version).

> [!NOTE]
> The bumping / updating of Rust dependencies is done in multiple steps:
>
> 1. Update the minimum Version in the root `Cargo.toml` manifest.
> 2. Run the `cargo update` command, which also updates the `Cargo.lock` file.
> 3. Lastly, run `make regenerate-nix` to update the `Cargo.nix` file.

### Bump Rust Dependencies

- [ ] Bump `stackable-operator` and friends
- [ ] Bump `product-config`
- [ ] Bump all other dependencies
