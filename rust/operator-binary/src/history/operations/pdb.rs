use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::pdb::{Error as PdbError, PodDisruptionBudgetBuilder},
    client::Client,
    cluster_resources::{ClusterResources, Error as ClusterresourceError},
    commons::pdb::PdbConfig,
    kube::ResourceExt,
};
use stackable_spark_k8s_crd::{
    constants::{APP_NAME, HISTORY_CONTROLLER_NAME, HISTORY_ROLE_NAME, OPERATOR_NAME},
    history::SparkHistoryServer,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot create PodDisruptionBudget for role [{role}]"))]
    CreatePdb { source: PdbError, role: String },
    #[snafu(display("Cannot apply PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: ClusterresourceError,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    history: &SparkHistoryServer,
    client: &Client,
    cluster_resources: &mut ClusterResources,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
    }
    let max_unavailable = pdb
        .max_unavailable
        .unwrap_or(max_unavailable_history_servers());
    let pdb = PodDisruptionBudgetBuilder::new_with_role(
        history,
        APP_NAME,
        HISTORY_ROLE_NAME,
        OPERATOR_NAME,
        HISTORY_CONTROLLER_NAME,
    )
    .with_context(|_| CreatePdbSnafu {
        role: HISTORY_ROLE_NAME,
    })?
    .with_max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_history_servers() -> u16 {
    1
}
