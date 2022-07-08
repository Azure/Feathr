use std::collections::HashMap;
use std::fmt::Debug;

use chrono::{Utc, DateTime};
use poem_openapi::{Enum, Object};
use registry_provider::EntityProperty;
use serde::{Deserialize, Serialize};

use super::{EntityAttributes, Relationship};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum EntityType {
    #[oai(rename = "unknown")]
    Unknown,

    #[oai(rename = "feathr_workspace_v1")]
    Project,
    #[oai(rename = "feathr_source_v1")]
    Source,
    #[oai(rename = "feathr_anchor_v1")]
    Anchor,
    #[oai(rename = "feathr_anchor_feature_v1")]
    AnchorFeature,
    #[oai(rename = "feathr_derived_feature_v1")]
    DerivedFeature,
}

impl From<registry_provider::EntityType> for EntityType {
    fn from(v: registry_provider::EntityType) -> Self {
        match v {
            registry_provider::EntityType::Unknown => EntityType::Unknown,
            registry_provider::EntityType::Project => EntityType::Project,
            registry_provider::EntityType::Source => EntityType::Source,
            registry_provider::EntityType::Anchor => EntityType::Anchor,
            registry_provider::EntityType::AnchorFeature => EntityType::AnchorFeature,
            registry_provider::EntityType::DerivedFeature => EntityType::DerivedFeature,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct Entity {
    pub guid: String,
    pub name: String,
    pub qualified_name: String,
    pub version: u64,
    #[oai(rename = "typeName")]
    pub entity_type: EntityType,
    pub status: String,
    pub display_text: String,
    pub labels: Vec<String>,
    pub attributes: EntityAttributes,
    pub created_by: String,
    pub created_on: DateTime<Utc>,
}

impl From<registry_provider::Entity<EntityProperty>> for Entity {
    fn from(v: registry_provider::Entity<EntityProperty>) -> Self {
        Self {
            guid: v.properties.guid.to_string(),
            name: v.name,
            qualified_name: v.qualified_name,
            version: v.version,
            entity_type: v.entity_type.into(),
            status: format!("{:?}", v.properties.status),
            display_text: v.properties.display_text.clone(),
            labels: v.properties.labels.clone(),
            created_by: v.properties.created_by.clone(),
            created_on: v.properties.created_on.clone(),
            attributes: v.properties.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
pub struct Entities {
    pub entities: Vec<Entity>,
}

impl FromIterator<registry_provider::Entity<EntityProperty>> for Entities {
    fn from_iter<T: IntoIterator<Item = registry_provider::Entity<EntityProperty>>>(
        iter: T,
    ) -> Self {
        Self {
            entities: iter.into_iter().map(|e| e.into()).collect(),
        }
    }
}

impl From<Vec<registry_provider::Entity<EntityProperty>>> for Entities {
    fn from(v: Vec<registry_provider::Entity<EntityProperty>>) -> Self {
        v.into_iter().collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityUniqueAttributes {
    pub qualified_name: String,
    pub version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityRef {
    guid: String,
    type_name: String,
    unique_attributes: EntityUniqueAttributes,
}

impl EntityRef {
    pub fn new<Prop>(e: &registry_provider::Entity<Prop>) -> Self
    where
        Prop: Clone + Debug + PartialEq + Eq,
    {
        Self {
            guid: e.id.to_string(),
            type_name: e.entity_type.get_name().to_string(),
            unique_attributes: EntityUniqueAttributes {
                qualified_name: e.qualified_name.to_owned(),
                version: e.version,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct EntityLineage {
    #[serde(rename = "guidEntityMap")]
    pub guid_entity_map: HashMap<String, Entity>,
    pub relations: Vec<Relationship>,
}

impl
    From<(
        Vec<registry_provider::Entity<EntityProperty>>,
        Vec<registry_provider::Edge>,
    )> for EntityLineage
{
    fn from(
        (entities, edges): (
            Vec<registry_provider::Entity<EntityProperty>>,
            Vec<registry_provider::Edge>,
        ),
    ) -> Self {
        let guid_entity_map: HashMap<String, Entity> = entities
            .into_iter()
            .map(|e| (e.id.to_string(), e.into()))
            .collect();
        Self {
            guid_entity_map,
            relations: edges.into_iter().map(|e| e.into()).collect(),
        }
    }
}

impl From<(Vec<Entity>, Vec<registry_provider::Edge>)> for EntityLineage {
    fn from((entities, edges): (Vec<Entity>, Vec<registry_provider::Edge>)) -> Self {
        let guid_entity_map: HashMap<String, Entity> =
            entities.into_iter().map(|e| (e.guid.clone(), e)).collect();
        Self {
            guid_entity_map,
            relations: edges.into_iter().map(|e| e.into()).collect(),
        }
    }
}
