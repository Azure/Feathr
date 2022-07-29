use registry_provider::{EntityProperty, Entity, Edge, RbacRecord};

use crate::Registry;

#[cfg(feature = "mssql")]
mod mssql;

#[cfg(feature = "ossdbms")]
mod sqlx;

fn get_entity_table() -> String {
    std::env::var("ENTITY_TABLE").unwrap_or_else(|_| "entities".to_string())
}

fn get_edge_table() -> String {
    std::env::var("EDGE_TABLE").unwrap_or_else(|_| "edges".to_string())
}

fn get_rbac_table() -> String {
    std::env::var("RBAC_TABLE").unwrap_or_else(|_| "userroles".to_string())
}

pub fn attach_storage(registry: &mut Registry<EntityProperty>) {
    #[cfg(feature = "mssql")]
    if mssql::validate_condition() {
        mssql::attach_storage(registry);
    }

    #[cfg(feature = "ossdbms")]
    if sqlx::validate_condition() {
        sqlx::attach_storage(registry);
    }
}

pub async fn load_content(
) -> Result<(Vec<Entity<EntityProperty>>, Vec<Edge>, Vec<RbacRecord>), anyhow::Error> {
    #[cfg(feature = "mssql")]
    if mssql::validate_condition() {
        return mssql::load_content().await;
    }

    #[cfg(feature = "ossdbms")]
    if sqlx::validate_condition() {
        return sqlx::load_content().await;
    }
    anyhow::bail!("Unable to load registry")
}
