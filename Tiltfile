default_registry("docker.stackable.tech/sandbox")

meta = read_json('nix/meta.json')
operator_name = meta['operator']['name']

custom_build(
    'docker.stackable.tech/sandbox/' + operator_name,
    'nix shell -f . crate2nix -c crate2nix generate && nix-build . -A docker --argstr dockerName "${EXPECTED_REGISTRY}/' + operator_name + '" && ./result/load-image | docker load',
    deps=['rust', 'Cargo.toml', 'Cargo.lock', 'default.nix', "nix", 'build.rs', 'vendor'],
    # ignore=['result*', 'Cargo.nix', 'target', *.yaml],
    outputs_image_ref_to='result/ref',
)

# Load the latest CRDs from Nix
watch_file('result')
if os.path.exists('result'):
   k8s_yaml('result/crds.yaml')

# Exclude stale CRDs from Helm chart, and apply the rest
helm_crds, helm_non_crds = filter_yaml(
   helm(
      'deploy/helm/' + operator_name,
      name=operator_name,
      set=[
         'image.repository=docker.stackable.tech/sandbox/' + operator_name,
      ],
   ),
   api_version = "^apiextensions\\.k8s\\.io/.*$",
   kind = "^CustomResourceDefinition$",
)
k8s_yaml(helm_non_crds)
