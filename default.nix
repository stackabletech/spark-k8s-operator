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
      krb5-sys = attrs: {
        nativeBuildInputs = [ pkgs.pkg-config ];
        buildInputs = [ pkgs.krb5 ];
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.glibc.dev}/include -I${pkgs.clang.cc.lib}/lib/clang/${pkgs.lib.getVersion pkgs.clang.cc}/include";
      };
      libgssapi-sys = attrs: {
        buildInputs = [ pkgs.krb5 ];
        LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
        BINDGEN_EXTRA_CLANG_ARGS = "-I${pkgs.glibc.dev}/include -I${pkgs.clang.cc.lib}/lib/clang/${pkgs.lib.getVersion pkgs.clang.cc}/include";
      };
      # FIXME: Remove when https://github.com/NixOS/nixpkgs/pull/266787 is merged.
      # See https://github.com/stackabletech/operator-templating/pull/289 for details.
      ring = attrs: {
        CARGO_MANIFEST_LINKS = attrs.links;
      };
    };
  }
, meta ? pkgs.lib.importJSON ./nix/meta.json
, dockerName ? "docker.stackable.tech/sandbox/${meta.operator.name}"
, dockerTag ? null
}:
rec {
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
}
