let
  self = import ./. {};
  inherit (self) sources pkgs meta;

  beku = pkgs.callPackage (sources."beku.py" + "/beku.nix") {};
  cargoDependencySetOfCrate = crate: [ crate ] ++ pkgs.lib.concatMap cargoDependencySetOfCrate (crate.dependencies ++ crate.buildDependencies);
  cargoDependencySet = pkgs.lib.unique (pkgs.lib.flatten (pkgs.lib.mapAttrsToList (crateName: crate: cargoDependencySetOfCrate crate.build) self.cargo.workspaceMembers));
in pkgs.mkShell rec {
  name = meta.operator.name;

  packages = with pkgs; [
    ## cargo et-al
    rustup # this breaks pkg-config if it is in the nativeBuildInputs

    ## Extra dependencies for use in a pure env (nix-shell --pure)
    ## These are mosuly useful for maintainers of this shell.nix
    ## to ensure all the dependencies are caught.
    # cacert
    # vim nvim nano
  ];

  # derivation runtime dependencies
  buildInputs = pkgs.lib.concatMap (crate: crate.buildInputs) cargoDependencySet;

  # build time dependencies
  nativeBuildInputs = pkgs.lib.concatMap (crate: crate.nativeBuildInputs) cargoDependencySet ++ (with pkgs; [
    beku
    docker
    gettext # for the proper envsubst
    git
    jq
    kind
    kubectl
    kubernetes-helm
    kuttl
    nix # this is implied, but needed in the pure env
    # tilt already defined in default.nix
    which
    yq-go
  ]);

  LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
  BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.glibc.dev}/include -I${pkgs.clang}/resource-root/include";
}
