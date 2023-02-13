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
    contents = [ pkgs.bashInteractive pkgs.coreutils pkgs.util-linuxMinimal ];
    config = {
    Env =
      let
        fileRefVars = {
          PRODUCT_CONFIG = deploy/config-spec/properties.yaml;
        };
      in lib.concatLists (lib.mapAttrsToList (env: path: lib.optional (lib.pathExists path) "${env}=${path}") fileRefVars);
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
