{ sources ? import ./nix/sources.nix # managed by https://github.com/nmattia/niv
, nixpkgs ? sources.nixpkgs
, pkgs ? import nixpkgs {}
, cargo ? import ./Cargo.nix {
    inherit nixpkgs pkgs; release = false;
    defaultCrateOverrides = pkgs.defaultCrateOverrides // {
      prost-build = attrs: {
        buildInputs = [ pkgs.protobuf ];
      };
      tonic-reflection = attrs: {
        buildInputs = [ pkgs.rustfmt ];
      };
      csi-grpc = attrs: {
        nativeBuildInputs = [ pkgs.protobuf ];
      };
      stackable-secret-operator = attrs: {
        buildInputs = [ pkgs.protobuf pkgs.rustfmt ];
      };
      stackable-opa-user-info-fetcher = attrs: {
        # TODO: why is this not pulled in via libgssapi-sys?
        buildInputs = [ pkgs.krb5 ];
      };
      krb5-sys = attrs: {
        nativeBuildInputs = [ pkgs.pkg-config ];
        buildInputs = [ pkgs.krb5 ];
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        # Clang's resource directory is located at ${pkgs.clang.cc.lib}/lib/clang/<version>.
        # Starting with Clang 16, only the major version is used for the resource directory,
        # whereas the full version was used in prior Clang versions (see
        # https://github.com/llvm/llvm-project/commit/e1b88c8a09be25b86b13f98755a9bd744b4dbf14).
        # The clang wrapper ${pkgs.clang} provides a symlink to the resource directory, which
        # we use instead.
        BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.glibc.dev}/include -I${pkgs.clang}/resource-root/include";
      };
      libgssapi-sys = attrs: {
        buildInputs = [ pkgs.krb5 ];
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.glibc.dev}/include -I${pkgs.clang}/resource-root/include";
      };
    };
  }
, meta ? pkgs.lib.importJSON ./nix/meta.json
, dockerName ? "oci.stackable.tech/sandbox/${meta.operator.name}"
, dockerTag ? null
}:
rec {
  inherit cargo sources pkgs meta;
  build = cargo.allWorkspaceMembers;
  entrypoint = build+"/bin/stackable-${meta.operator.name}";
  crds = pkgs.runCommand "${meta.operator.name}-crds.yaml" {}
  ''
    ${entrypoint} crd > $out
  '';

  dockerImage = pkgs.dockerTools.streamLayeredImage {
    name = dockerName;
    tag = dockerTag;
    contents = [
      # Common debugging tools
      pkgs.bashInteractive pkgs.coreutils pkgs.util-linuxMinimal
      # Kerberos 5 must be installed globally to load plugins correctly
      pkgs.krb5
      # Make the whole cargo workspace available on $PATH
      build
    ];
    config = {
      Env =
        let
          fileRefVars = {
            PRODUCT_CONFIG = deploy/config-spec/properties.yaml;
          };
        in pkgs.lib.concatLists (pkgs.lib.mapAttrsToList (env: path: pkgs.lib.optional (pkgs.lib.pathExists path) "${env}=${path}") fileRefVars);
      Entrypoint = [ entrypoint ];
      Cmd = [ "run" ];
    };
  };
  docker = pkgs.linkFarm "listener-operator-docker" [
    {
      name = "load-image";
      path = dockerImage;
    }
    {
      name = "ref";
      path = pkgs.writeText "${dockerImage.name}-image-tag" "${dockerImage.imageName}:${dockerImage.imageTag}";
    }
    {
      name = "image-repo";
      path = pkgs.writeText "${dockerImage.name}-repo" dockerImage.imageName;
    }
    {
      name = "image-tag";
      path = pkgs.writeText "${dockerImage.name}-tag" dockerImage.imageTag;
    }
    {
      name = "crds.yaml";
      path = crds;
    }
  ];

  # need to use vendored crate2nix because of https://github.com/kolloch/crate2nix/issues/264
  crate2nix = import sources.crate2nix {};
  tilt = pkgs.tilt;

  regenerateNixLockfiles = pkgs.writeScriptBin "regenerate-nix-lockfiles"
  ''
    #!/usr/bin/env bash
    set -euo pipefail
    echo Running crate2nix
    ${crate2nix}/bin/crate2nix generate

    # crate2nix adds a trailing newline (see
    # https://github.com/nix-community/crate2nix/commit/5dd04e6de2fbdbeb067ab701de8ec29bc228c389).
    # The pre-commit hook trailing-whitespace wants to remove it again
    # (see https://github.com/pre-commit/pre-commit-hooks?tab=readme-ov-file#trailing-whitespace).
    # So, remove the trailing newline already here to avoid that an
    # unnecessary change is shown in Git.
    sed -i '$d' Cargo.nix
  '';
}
