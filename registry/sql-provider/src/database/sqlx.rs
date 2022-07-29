use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::debug;
use sqlx::{
    any::AnyKind, pool::PoolConnection, Any, AnyConnection, AnyPool, ConnectOptions, Connection,
    Executor, FromRow,
};

use crate::{
    database::{get_entity_table, get_rbac_table},
    db_registry::ExternalStorage,
    Registry,
};
use common_utils::Logged;
use registry_provider::{
    Credential, Edge, EdgeType, Entity, EntityProperty, Permission, RbacRecord, RegistryError,
    Resource,
};
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;

use super::get_edge_table;

#[derive(sqlx::FromRow)]
struct EntityPropertyWrapper {
    entity_content: String,
}

async fn load_entities() -> Result<Vec<EntityProperty>, anyhow::Error> {
    let entities_table = get_entity_table();
    debug!("Loading entities from {}", entities_table);
    let pool = POOL
        .get_or_init(|| async { init_pool().await.ok() })
        .await
        .clone()
        .ok_or_else(|| anyhow::Error::msg("Environment variable 'CONNECTION_STR' is not set."))?;
    debug!("SQLx connection pool acquired, connecting to database");
    let sql = format!("SELECT entity_content from {}", entities_table);
    let rows = sqlx::query_as::<_, EntityPropertyWrapper>(&sql)
        .fetch_all(&pool)
        .await?;
    debug!("{} rows loaded", rows.len());
    let x = rows
        .into_iter()
        .map(|r| {
            debug!("{}", r.entity_content);
            serde_json::from_str::<EntityProperty>(
                &r.entity_content.replace('\n', "").replace('\r', ""),
            )
            .map_err(|e| {
                anyhow::Error::msg(format!(
                    "Failed to parse entity content: '{}', error is {}",
                    &r.entity_content,
                    e.to_string()
                ))
            })
            .log()
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    debug!("{} entities loaded", x.len());
    Ok(x)
}

#[derive(sqlx::FromRow)]
struct EdgeWrapper {
    from_id: String,
    to_id: String,
    edge_type: String,
}

async fn load_edges() -> Result<Vec<Edge>, anyhow::Error> {
    let edges_table = std::env::var("EDGE_TABLE").unwrap_or_else(|_| "edges".to_string());
    debug!("Loading edges from {}", edges_table);
    let pool = POOL
        .get_or_init(|| async { init_pool().await.ok() })
        .await
        .clone()
        .ok_or_else(|| anyhow::Error::msg("Environment variable 'CONNECTION_STR' is not set."))?;
    debug!("SQLx connection pool acquired, connecting to database");
    let sql = format!("SELECT from_id, to_id, edge_type from {}", edges_table);
    let rows: Vec<EdgeWrapper> = sqlx::query_as::<_, EdgeWrapper>(&sql)
        .fetch_all(&pool)
        .await?;
    debug!("{} rows loaded", rows.len());
    let x = rows
        .into_iter()
        .map(|r| -> Result<Edge, anyhow::Error> {
            let edge_type = match serde_json::from_str::<EdgeType>(&format!("\"{}\"", &r.edge_type))
            {
                Ok(v) => v,
                Err(e) => {
                    return Err(anyhow::Error::msg(format!(
                        "Failed to parse edge type: {}, error {}",
                        r.edge_type,
                        e.to_string()
                    )));
                }
            };
            let from = match Uuid::parse_str(&r.from_id) {
                Ok(v) => v,
                Err(e) => {
                    return Err(anyhow::Error::msg(format!(
                        "Failed to parse from id: {}, error {}",
                        r.from_id,
                        e.to_string()
                    )));
                }
            };
            let to = match Uuid::parse_str(&r.to_id) {
                Ok(v) => v,
                Err(e) => {
                    return Err(anyhow::Error::msg(format!(
                        "Failed to parse to id: {}, error {}",
                        r.to_id,
                        e.to_string()
                    )));
                }
            };

            Ok(Edge {
                edge_type,
                from,
                to,
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    debug!("{} edges loaded", x.len());
    Ok(x)
}

#[derive(FromRow)]
struct RbacEntry {
    user: String,
    resource: String,
    permission: String,
    requestor: String,
    reason: String,
    time: String,
}

async fn load_permissions() -> Result<Vec<RbacRecord>, anyhow::Error> {
    let permissions_table = get_rbac_table();
    debug!("Loading RBAC from {}", permissions_table);
    let pool = POOL
        .get_or_init(|| async { init_pool().await.ok() })
        .await
        .clone()
        .ok_or_else(|| anyhow::Error::msg("Environment variable 'CONNECTION_STR' is not set."))?;
    debug!("SQLx connection pool acquired, connecting to database");
    let sql = format!(
        r#"SELECT project_name, user_name, role_name from {}
        where delete_by is null
        "#,
        permissions_table
    );
    let rows: Vec<RbacEntry> = sqlx::query_as::<_, RbacEntry>(&sql)
        .fetch_all(&pool)
        .await?;
    debug!("{} rows loaded", rows.len());
    rows.into_iter()
        .map(|entry| {
            let credential = match entry.user.parse::<Uuid>() {
                Ok(id) => Credential::App(id),
                Err(_) => Credential::User(entry.user),
            };
            let resource = match entry.resource.as_str() {
                "global" => Resource::Global,
                _ => Resource::NamedEntity(entry.resource),
            };
            let permission = match entry.permission.as_str() {
                "consumer" => Permission::Read,
                "producer" => Permission::Write,
                "admin" => Permission::Admin,
                _ => Permission::Read,
            };
            let requestor = match entry.requestor.parse::<Uuid>() {
                Ok(id) => Credential::App(id),
                Err(_) => Credential::User(entry.requestor),
            };
            let reason = entry.reason;
            let time: DateTime<Utc> = DateTime::parse_from_str(&entry.time, "%Y-%m-%d %H:%M:%S")
                .map_err(|e| anyhow::Error::new(e))?
                .with_timezone(&Utc);
            Ok(RbacRecord {
                credential,
                resource,
                permission,
                requestor,
                reason,
                time,
            })
        })
        .collect()
}

pub async fn load_content(
) -> Result<(Vec<Entity<EntityProperty>>, Vec<Edge>, Vec<RbacRecord>), anyhow::Error> {
    let conn_str = std::env::var("CONNECTION_STR")?;
    if conn_str
        .parse::<<AnyConnection as Connection>::Options>()?
        .kind()
        == AnyKind::Sqlite
    {
        // HACK:
        // Ensure the database file is created.
        // For unknown reason AnyConnection doesn't create the database file
        let mut conn = sqlx::sqlite::SqliteConnectOptions::from_str(&conn_str)?
            .create_if_missing(true)
            .connect()
            .await?;

        debug!("Using SQLite database, creating schema...");
        debug!(
            "Creating entities table '{}' if not exists",
            get_entity_table()
        );
        let sql = &format!(
            r#"CREATE TABLE IF NOT EXISTS {}
            (entity_id varchar(50), entity_content text, PRIMARY KEY (entity_id))
            "#,
            get_entity_table()
        );
        conn.execute(sqlx::query(&sql)).await?;

        debug!("Creating edges table '{}' if not exists", get_edge_table());
        let sql = &format!(
            r#"CREATE TABLE IF NOT EXISTS {}
            (from_id varchar(50), to_id varchar(50), edge_type varchar(50), PRIMARY KEY (from_id, to_id, edge_type))"#,
            get_edge_table()
        );
        conn.execute(sqlx::query(&sql)).await?;

        conn.close().await?;
    }

    debug!("Loading registry data from database");
    let edges = load_edges().await?;
    let entities = load_entities().await?;
    let permissions = load_permissions().await?;
    debug!(
        "{} entities and {} edges loaded",
        entities.len(),
        edges.len()
    );
    Ok((
        entities.into_iter().map(|e| e.into()).collect(),
        edges.into_iter().map(|e| e.into()).collect(),
        permissions,
    ))
}

pub fn validate_condition() -> bool {
    if let Ok(conn_str) = std::env::var("CONNECTION_STR") {
        conn_str
            .parse::<<AnyConnection as Connection>::Options>()
            .is_ok()
    } else {
        false
    }
}

pub fn attach_storage(registry: &mut Registry<EntityProperty>) {
    registry
        .external_storage
        .push(Arc::new(RwLock::new(SqlxStorage::default())));
}

static POOL: OnceCell<Option<AnyPool>> = OnceCell::const_new();

async fn init_pool() -> anyhow::Result<AnyPool> {
    debug!("Initializing SQLx connection pool");
    let conn_str = std::env::var("CONNECTION_STR")?;
    let pool = AnyPool::connect(conn_str.as_str()).await?;
    debug!("SQLx connection pool initialized");
    Ok(pool)
}

async fn connect() -> Result<PoolConnection<Any>, anyhow::Error> {
    debug!("Acquiring SQLx connection pool");
    let pool = POOL
        .get_or_init(|| async { init_pool().await.ok() })
        .await
        .clone()
        .ok_or_else(|| anyhow::Error::msg("Environment variable 'CONNECTION_STR' is not set."))?;
    debug!("SQLx connection pool acquired, connecting to database");
    let conn = pool.acquire().await?;
    debug!("Database connected");
    Ok(conn)
}

#[derive(Debug)]
struct SqlxStorage {
    entity_table: String,
    edge_table: String,
}

impl SqlxStorage {
    pub fn new(entity_table: &str, edge_table: &str) -> Self {
        Self {
            entity_table: entity_table.to_string(),
            edge_table: edge_table.to_string(),
        }
    }
}

impl Default for SqlxStorage {
    fn default() -> Self {
        Self::new(&get_entity_table(), &get_edge_table())
    }
}

#[async_trait]
impl ExternalStorage<EntityProperty> for SqlxStorage {
    /**
     * Function will be called when a new entity is added in the graph
     * ExternalStorage may need to create the entity record in database, etc
     */
    async fn add_entity(
        &mut self,
        id: Uuid,
        entity: &Entity<EntityProperty>,
    ) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        let kind = conn.kind();
        match kind {
            sqlx::any::AnyKind::Postgres => {
                let sql = &format!(
                    r#"INSERT INTO {}
                    (entity_id, entity_content)
                    values
                    ($1, $2)
                    ON CONFLICT DO NOTHING;"#,
                    self.entity_table,
                );
                let query = sqlx::query(&sql)
                    .bind(id.to_string())
                    .bind(serde_json::to_string_pretty(&entity.properties).unwrap());
                conn.execute(query)
                    .await
                    .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
            }
            sqlx::any::AnyKind::MySql => {
                let sql = format!(
                    r#"INSERT IGNORE INTO {}
                    (entity_id, entity_content)
                    values
                    (?, ?)"#,
                    self.entity_table,
                );
                let query = sqlx::query(&sql)
                    .bind(id.to_string())
                    .bind(serde_json::to_string_pretty(&entity.properties).unwrap());
                conn.execute(query)
                    .await
                    .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
            }
            sqlx::any::AnyKind::Sqlite => {
                let sql = format!(
                    r#"INSERT OR IGNORE INTO {}
                    (entity_id, entity_content)
                    values
                    (?, ?)"#,
                    self.entity_table,
                );
                let query = sqlx::query(&sql)
                    .bind(id.to_string())
                    .bind(serde_json::to_string_pretty(&entity.properties).unwrap());
                conn.execute(query)
                    .await
                    .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
            }
        };
        Ok(())
    }

    /**
     * Function will be called when an entity is deleted in the graph
     * ExternalStorage may need to remove the entity record from database, etc
     */
    async fn delete_entity(
        &mut self,
        id: Uuid,
        _entity: &Entity<EntityProperty>,
    ) -> Result<(), RegistryError> {
        let sql = format!(r#"DELETE {} WHERE entity_id = ?;"#, self.entity_table,);
        let query = sqlx::query(&sql).bind(id.to_string());
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(query)
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    /**
     * Function will be called when 2 entities are connected.
     * EntityProp has already been updated accordingly.
     * ExternalStorage may need to create the edge record in database, etc
     */
    async fn connect(
        &mut self,
        from_id: Uuid,
        to_id: Uuid,
        edge_type: EdgeType,
    ) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        let kind = conn.kind();
        match kind {
            sqlx::any::AnyKind::Postgres => {
                let sql = &format!(
                    r#"INSERT INTO {}
                    (from_id, to_id, edge_type)
                    values
                    ($1, $2, $3)
                    ON CONFLICT DO NOTHING;"#,
                    self.edge_table,
                );
                let query = sqlx::query(&sql)
                    .bind(from_id.to_string())
                    .bind(to_id.to_string())
                    .bind(format!("{:?}", edge_type));
                conn.execute(query)
                    .await
                    .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
            }
            sqlx::any::AnyKind::MySql => {
                let sql = format!(
                    r#"INSERT IGNORE INTO {}
                    (from_id, to_id, edge_type)
                    values
                    (?, ?, ?)"#,
                    self.edge_table,
                );
                let query = sqlx::query(&sql)
                    .bind(from_id.to_string())
                    .bind(to_id.to_string())
                    .bind(format!("{:?}", edge_type));
                conn.execute(query)
                    .await
                    .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
            }
            sqlx::any::AnyKind::Sqlite => {
                let sql = format!(
                    r#"INSERT OR IGNORE INTO {}
                    (from_id, to_id, edge_type)
                    values
                    (?, ?, ?)"#,
                    self.edge_table,
                );
                let query = sqlx::query(&sql)
                    .bind(from_id.to_string())
                    .bind(to_id.to_string())
                    .bind(format!("{:?}", edge_type));
                conn.execute(query)
                    .await
                    .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
            }
        };
        Ok(())
    }

    /**
     * Function will be called when 2 entities are disconnected.
     * EntityProp has already been updated accordingly.
     * ExternalStorage may need to remove the edge record from database, etc
     */
    async fn disconnect(
        &mut self,
        _from: &Entity<EntityProperty>,
        from_id: Uuid,
        _to: &Entity<EntityProperty>,
        to_id: Uuid,
        edge_type: EdgeType,
        _edge_id: Uuid,
    ) -> Result<(), RegistryError> {
        let sql = format!(
            r#"DELETE {} WHERE from_id=? and to_id=? and edge_type=?;"#,
            self.edge_table,
        );
        let query = sqlx::query(&sql)
            .bind(from_id.to_string())
            .bind(to_id.to_string())
            .bind(format!("{:?}", edge_type));
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(query)
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn grant_permission(&mut self, grant: &RbacRecord) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        let now = match conn.kind() {
            AnyKind::Postgres => "NOW()",
            AnyKind::MySql => "NOW()",
            AnyKind::Sqlite => "datetime('now')",
        };
        let sql = format!(
            "INSERT INTO {}
            (user_name, role_name, project_name, create_by, create_reason, create_time)
            values
            (?, ?, ?, ?, ?, {})",
            get_rbac_table(),
            now,
        );
        let query = sqlx::query(&sql)
            .bind(grant.credential.to_string())
            .bind(grant.permission.to_string())
            .bind(grant.resource.to_string())
            .bind(grant.requestor.to_string())
            .bind(grant.reason.clone());
        conn.execute(query)
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn revoke_permission(&mut self, revoke: &RbacRecord) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        let now = match conn.kind() {
            AnyKind::Postgres => "NOW()",
            AnyKind::MySql => "NOW()",
            AnyKind::Sqlite => "datetime('now')",
        };
        let sql = format!(
            "UPDATE {}
            SET delete_by=?, delete_reason=?, delete_time={}
            WHERE user_name = ? and role_name = ? and project_name = ? and deleted_reason is null",
            get_rbac_table(),
            now,
        );
        let query = sqlx::query(&sql)
            .bind(revoke.requestor.to_string())
            .bind(revoke.reason.clone())
            .bind(revoke.credential.to_string())
            .bind(revoke.permission.to_string())
            .bind(revoke.resource.to_string());
        conn.execute(query)
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }
}
