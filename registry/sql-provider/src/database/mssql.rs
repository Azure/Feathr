use std::sync::Arc;

use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use common_utils::{Appliable, Logged};
use log::debug;
use tiberius::{FromSql, Row};
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;

use registry_provider::{Edge, EdgeType, Entity, EntityProperty, RegistryError};

use crate::{db_registry::ExternalStorage, Registry};

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

async fn load_entities(
    conn: &mut PooledConnection<'static, ConnectionManager>,
) -> Result<Vec<EntityProperty>, anyhow::Error> {
    let entities_table =
        std::env::var("MSSQL_ENTITY_TABLE").unwrap_or_else(|_| "entities".to_string());
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
    let edges_table = std::env::var("MSSQL_EDGE_TABLE").unwrap_or_else(|_| "edges".to_string());
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
    // TODO:
    true
}

pub async fn load_registry() -> Result<Registry<EntityProperty>, anyhow::Error> {
    debug!("Loading registry data from database");
    let mut conn = connect().await?;
    let edges = load_edges(&mut conn).await?;
    let entities = load_entities(&mut conn).await?;
    debug!(
        "{} entities and {} edges loaded",
        entities.len(),
        edges.len()
    );
    let mut registry = Registry::load(
        entities.into_iter().map(|e| e.into()),
        edges.into_iter().map(|e| e.into()),
    )
    .await?;
    registry
        .external_storage
        .push(Arc::new(RwLock::new(MsSqlStorage::default())));

    Ok(registry)
}

pub async fn load_content() -> Result<(Vec<Entity<EntityProperty>>, Vec<Edge>), anyhow::Error> {
    debug!("Loading registry data from database");
    let mut conn = connect().await?;
    let edges = load_edges(&mut conn).await?;
    let entities = load_entities(&mut conn).await?;
    debug!(
        "{} entities and {} edges loaded",
        entities.len(),
        edges.len()
    );
    Ok((
        entities.into_iter().map(|e| e.into()).collect(),
        edges.into_iter().map(|e| e.into()).collect(),
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
        Self::new(
            &std::env::var("MSSQL_ENTITY_TABLE").unwrap_or_else(|_| "entities".to_string()),
            &std::env::var("MSSQL_EDGE_TABLE").unwrap_or_else(|_| "edges".to_string()),
        )
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
