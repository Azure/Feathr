use std::sync::Arc;

use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use chrono::{DateTime, Utc};
use common_utils::{Appliable, Logged};
use tiberius::{FromSql, Row};
use tiberius_derive::FromRow;
use tokio::sync::{OnceCell, RwLock};
use tracing::{debug, warn};
use uuid::Uuid;

use registry_provider::{
    Credential, Edge, EdgeType, Entity, EntityProperty, Permission, RbacRecord, RegistryError,
    Resource,
};

use crate::{
    database::{get_entity_table, get_rbac_table},
    db_registry::ExternalStorage,
    Registry,
};

use super::get_edge_table;

fn edge_try_from_row(r: Row) -> Result<Edge, tiberius::error::Error> {
    let c: Option<&str> = r.get(0);
    let from = Uuid::parse_str(c.unwrap_or_default())
        .map_err(|e| tiberius::error::Error::Conversion(format!("{:?}", e).into()))?;
    let c: Option<&str> = r.get(1);
    let to = Uuid::parse_str(c.unwrap_or_default())
        .map_err(|e| tiberius::error::Error::Conversion(format!("{:?}", e).into()))?;
    let s = r
        .get::<&str, usize>(2)
        .map(|s| format!("\"{}\"", s))
        .ok_or_else(|| tiberius::error::Error::Conversion("".into()))?;
    let edge_type: EdgeType = serde_json::from_str::<EdgeType>(&s)
        .ok()
        .ok_or_else(|| tiberius::error::Error::Conversion("".into()))?;
    Ok(Edge {
        from,
        to,
        edge_type,
    })
}

struct EntityPropertyWrapper(EntityProperty);

impl<'a> FromSql<'a> for EntityPropertyWrapper {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        match value {
            tiberius::ColumnData::String(s) => s
                .to_owned()
                .map(|s| serde_json::from_str::<EntityProperty>(&s).log().ok())
                .map(|e| e.map(EntityPropertyWrapper))
                .ok_or_else(|| tiberius::error::Error::Conversion("".into())),
            _ => Err(tiberius::error::Error::Conversion("".into())),
        }
    }
}

#[derive(FromRow)]
#[tiberius_derive(owned)]
struct RbacEntry {
    user: String,
    resource: String,
    permission: String,
    requestor: String,
    reason: String,
    time: String,
}

async fn load_entities(
    conn: &mut PooledConnection<'static, ConnectionManager>,
) -> Result<Vec<EntityProperty>, anyhow::Error> {
    let entities_table = get_entity_table();
    debug!("Loading entities from {}", entities_table);
    let x: Vec<EntityProperty> = conn
        .simple_query(format!("SELECT entity_content from {}", entities_table))
        .await?
        .into_first_result()
        .await?
        .into_iter()
        .filter_map(|r| r.get::<EntityPropertyWrapper, usize>(0).map(|e| e.0))
        .collect();
    debug!("{} entities loaded", x.len());
    Ok(x)
}

async fn load_edges(
    conn: &mut PooledConnection<'static, ConnectionManager>,
) -> Result<Vec<Edge>, anyhow::Error> {
    let edges_table = std::env::var("EDGE_TABLE").unwrap_or_else(|_| "edges".to_string());
    debug!("Loading edges from {}", edges_table);
    let x: Vec<Edge> = conn
        .simple_query(format!(
            "SELECT from_id, to_id, edge_type from {}",
            edges_table
        ))
        .await?
        .into_first_result()
        .await?
        .into_iter()
        .filter_map(|r| edge_try_from_row(r).ok())
        .collect();
    debug!("{} edges loaded", x.len());
    Ok(x)
}

async fn load_permissions(
    conn: &mut PooledConnection<'static, ConnectionManager>,
) -> Result<Vec<RbacRecord>, anyhow::Error> {
    let permissions_table = get_rbac_table();
    {
        let check = conn
            .simple_query(format!("select 1 from {}", permissions_table))
            .await;
        if check.is_err() {
            warn!("Permissions table not found, RBAC is disabled");
            std::env::remove_var("ENABLE_RBAC");
            return Ok(vec![]);
        }
    }
    debug!("Loading RBAC from {}", permissions_table);
    let x: Vec<RbacEntry> = conn
        .simple_query(format!(
            r#"SELECT user_name, project_name, role_name, create_by, create_reason, CONVERT(NVARCHAR(max), create_time, 20) from {}
            where delete_by is null
            order by record_id"#,
            permissions_table
        ))
        .await?
        .into_first_result()
        .await?
        .into_iter()
        .map(RbacEntry::from_row)
        .collect::<Result<Vec<_>, _>>()?;
    debug!("{} permissions loaded", x.len());
    x.into_iter()
        .map(|entry| {
            let credential = match entry.user.parse::<Uuid>() {
                Ok(id) => Credential::App(id),
                Err(_) => Credential::User(entry.user),
            };
            let resource = match entry.resource.as_str() {
                "global" => Resource::Global,
                _ => Resource::NamedEntity(entry.resource),
            };
            let permission = match entry.permission.to_lowercase().as_str() {
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
                .map(|t| t.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
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

static POOL: OnceCell<Option<Arc<RwLock<Pool<ConnectionManager>>>>> = OnceCell::const_new();

async fn init_pool() -> anyhow::Result<Arc<RwLock<Pool<ConnectionManager>>>> {
    debug!("Initializing MSSQL connection pool");
    let conn_str = std::env::var("CONNECTION_STR")?;
    let mgr = bb8_tiberius::ConnectionManager::build(conn_str.as_str())?;
    let pool = bb8::Pool::builder().max_size(5).build(mgr).await?;
    debug!("MSSQL connection pool initialized");
    Ok(Arc::new(RwLock::new(pool)))
}

async fn connect() -> Result<PooledConnection<'static, ConnectionManager>, anyhow::Error> {
    debug!("Acquiring MSSQL connection pool");
    let pool = POOL
        .get_or_init(|| async { init_pool().await.ok() })
        .await
        .clone()
        .ok_or_else(|| anyhow::Error::msg("Environment variable 'CONNECTION_STR' is not set."))?;
    debug!("MSSQL connection pool acquired, connecting to database");
    let conn = pool.read().await.get_owned().await?;
    debug!("Database connected");
    Ok(conn)
}

pub fn validate_condition() -> bool {
    if let Ok(conn_str) = std::env::var("CONNECTION_STR") {
        tiberius::Config::from_ado_string(&conn_str).is_ok()
    } else {
        false
    }
}

pub async fn load_content(
) -> Result<(Vec<Entity<EntityProperty>>, Vec<Edge>, Vec<RbacRecord>), anyhow::Error> {
    debug!("Loading registry data from database");
    let mut conn = connect().await?;
    let edges = load_edges(&mut conn).await?;
    let entities = load_entities(&mut conn).await?;
    let permissions = load_permissions(&mut conn).await?;
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

pub fn attach_storage(registry: &mut Registry<EntityProperty>) {
    registry
        .external_storage
        .push(Arc::new(RwLock::new(MsSqlStorage::default())));
}

#[derive(Debug)]
pub struct MsSqlStorage {
    entity_table: String,
    edge_table: String,
}

impl MsSqlStorage {
    pub fn new(entity_table: &str, edge_table: &str) -> Self {
        Self {
            entity_table: entity_table.to_string(),
            edge_table: edge_table.to_string(),
        }
    }
}

impl Default for MsSqlStorage {
    fn default() -> Self {
        Self::new(&get_entity_table(), &get_edge_table())
    }
}

#[async_trait]
impl ExternalStorage<EntityProperty> for MsSqlStorage {
    async fn add_entity(
        &mut self,
        id: Uuid,
        entity: &Entity<EntityProperty>,
    ) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(
            format!(
                r#"IF NOT EXISTS (SELECT 1 FROM {} WHERE entity_id = @P1)
                BEGIN
                    INSERT INTO {}
                    (entity_id, entity_content)
                    values
                    (@P1, @P2)
                END"#,
                self.entity_table, self.entity_table,
            )
            .apply(|s| {
                debug!("SQL is: {}", s);
                debug!("Id: {}", &id);
                debug!("Name: {}", &entity.qualified_name);
                s
            }),
            &[
                &id.to_string(),
                &serde_json::to_string_pretty(&entity.properties).unwrap(),
            ],
        )
        .await
        .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn delete_entity(
        &mut self,
        id: Uuid,
        _entity: &Entity<EntityProperty>,
    ) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(
            format!("DELETE {} WHERE entity_id = @P1", self.entity_table).apply(|s| {
                debug!("SQL is: {}", s);
                s
            }),
            &[&id.to_string()],
        )
        .await
        .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn connect(
        &mut self,
        from_id: Uuid,
        to_id: Uuid,
        edge_type: EdgeType,
    ) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(
            format!(
                r#"IF NOT EXISTS (SELECT 1 FROM {} WHERE from_id=@P1 and to_id=@P2 and edge_type=@P3)
                BEGIN
                    INSERT INTO {}
                    (from_id, to_id, edge_type)
                    values
                    (@P1, @P2, @P3)
                END"#,
                self.edge_table, self.edge_table
            )
            .apply(|s| {
                debug!("SQL is: {}", s);
                s
            }),
            &[
                &from_id.to_string(),
                &to_id.to_string(),
                &format!("{:?}", edge_type),
            ],
        )
        .await
        .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn disconnect(
        &mut self,
        _from: &Entity<EntityProperty>,
        from_id: Uuid,
        _to: &Entity<EntityProperty>,
        to_id: Uuid,
        edge_type: EdgeType,
        _edge_id: Uuid,
    ) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(
            format!(
                "DELETE {} WHERE from_id=@P1 and to_id=@P2 and edge_type=@P3",
                self.edge_table
            )
            .apply(|s| {
                debug!("SQL is: {}", s);
                s
            }),
            &[
                &from_id.to_string(),
                &to_id.to_string(),
                &format!("{:?}", edge_type),
            ],
        )
        .await
        .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn grant_permission(&mut self, grant: &RbacRecord) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(
            format!(
                "INSERT INTO {}
                (user_name, role_name, project_name, create_by, create_reason, create_time)
                values
                (@P1, @P2, @P3, @P4, @P5, SYSUTCDATETIME())",
                get_rbac_table()
            )
            .apply(|s| {
                debug!("SQL is: {}", s);
                s
            }),
            &[
                &grant.credential.to_string(),
                &grant.permission.to_string(),
                &grant.resource.to_string(),
                &grant.requestor.to_string(),
                &grant.reason,
            ],
        )
        .await
        .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }

    async fn revoke_permission(&mut self, revoke: &RbacRecord) -> Result<(), RegistryError> {
        let mut conn = connect()
            .await
            .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        conn.execute(
            format!(
                "UPDATE {}
                SET delete_by=@P1, delete_reason=@P2, delete_time=SYSUTCDATETIME()
                WHERE user_name = @P3 and role_name = @P4 and project_name = @P5 and delete_reason is null",
                get_rbac_table()
            )
            .apply(|s| {
                debug!("SQL is: {}", s);
                debug!("P1={}", &revoke.requestor.to_string());
                debug!("P2={}", &revoke.reason);
                debug!("P3={}", &revoke.credential.to_string());
                debug!("P4={}", &revoke.permission.to_string());
                debug!("P5={}", &revoke.resource.to_string());
                s
            }),
            &[
                &revoke.requestor.to_string(),
                &revoke.reason,
                &revoke.credential.to_string(),
                &revoke.permission.to_string(),
                &revoke.resource.to_string(),
            ],
        )
        .await
        .map_err(|e| RegistryError::ExternalStorageError(format!("{:?}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs::File};

    use registry_provider::*;
    use serde::Deserialize;

    use crate::*;

    pub async fn load() -> Registry<EntityProperty> {
        #[derive(Debug, Deserialize)]
        struct SampleData {
            #[serde(rename = "guidEntityMap")]
            guid_entity_map: HashMap<Uuid, EntityProperty>,
            #[serde(rename = "relations")]
            relations: Vec<Edge>,
        }
        let f = File::open("../test-data/sample.json").unwrap();
        let data: SampleData = serde_json::from_reader(f).unwrap();
        let mut r = Registry::<EntityProperty>::load(
            data.guid_entity_map.into_iter().map(|(_, i)| i.into()),
            data.relations.into_iter().map(|i| i.into()),
            vec![].into_iter(),
        )
        .await
        .unwrap();
        let project = r.get_projects()[0].id;
        let subs: Vec<Uuid> = r
            .get_entities(|w| {
                w.entity_type == EntityType::AnchorFeature
                    || w.entity_type == EntityType::DerivedFeature
                    || w.entity_type == EntityType::Anchor
                    || w.entity_type == EntityType::Source
            })
            .into_iter()
            .map(|e| e.id)
            .collect();
        for sub in subs {
            r.connect(sub, project, EdgeType::BelongsTo).await.unwrap();
        }
        r
    }

    #[tokio::test]
    async fn test_dump() {
        let r = load().await;
        r.graph.node_weights().for_each(|w| {
            println!(
                "insert into entities_new (entity_id, entity_content) values ('{}', '{}');",
                w.id,
                serde_json::to_string(&w.properties).unwrap()
            );
        });
        println!("-- ---------------------------");
        r.graph.edge_weights().for_each(|w| {
            println!("insert into edges_new (edge_id, from_id, to_id, edge_type) values ('{}', '{}', '{}', '{:?}');", Uuid::new_v4(), w.from, w.to, w.edge_type);
        });
    }
}
