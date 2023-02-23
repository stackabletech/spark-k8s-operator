use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR is required"));
    built::write_built_file_with_opts(
        // built's env module depends on a whole bunch of variables that crate2nix doesn't provide
        // so we grab the specific env variables that we care about out ourselves instead.
        built::Options::default().set_env(false),
        "Cargo.toml".as_ref(),
        &out_dir.join("built.rs"),
    )
    .unwrap();
}
