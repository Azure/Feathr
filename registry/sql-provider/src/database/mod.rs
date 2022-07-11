use registry_provider::{EntityProperty, Entity, Edge};

use crate::Registry;

#[cfg(feature = "mssql")]
mod mssql;

#[cfg(feature = "ossdbms")]
mod sqlx;

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
) -> Result<(Vec<Entity<EntityProperty>>, Vec<Edge>), anyhow::Error> {
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
