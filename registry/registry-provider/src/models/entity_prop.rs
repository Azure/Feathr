use std::{collections::HashMap, fmt::Debug};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureAttributes, AnchorFeatureDef, Attributes, DerivedFeatureAttributes,
    DerivedFeatureDef, Entity, EntityPropMutator, EntityType, ProjectDef, RegistryError,
    SourceAttributes, SourceDef,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityStatus {
    Active,
    Deprecated,
}

#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityProperty {
    pub guid: Uuid,
    pub name: String,
    pub qualified_name: String,
    pub status: EntityStatus,
    pub display_text: String,
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub tags: HashMap<String, String>,
    pub version: u64,
    pub created_by: String,
    pub created_on: DateTime<Utc>,
    #[serde(flatten)]
    pub attributes: Attributes,
}

impl PartialEq for EntityProperty {
    fn eq(&self, other: &Self) -> bool {
        self.qualified_name == other.qualified_name && self.attributes == other.attributes
    }
}

impl EntityPropMutator for EntityProperty {
    fn new_project(definition: &ProjectDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            qualified_name: definition.qualified_name.to_owned(),
            name: definition.qualified_name.to_owned(),
            tags: definition.tags.to_owned(),
            status: EntityStatus::Active,
            display_text: definition.qualified_name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::Project,
            version: 0,
            created_by: definition.created_by.to_owned(),
            created_on: Utc::now(),
        })
    }
    fn new_source(definition: &SourceDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            qualified_name: definition.qualified_name.to_owned(),
            name: definition.name.to_owned(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            tags: definition.tags.to_owned(),
            attributes: Attributes::Source(SourceAttributes {
                path: definition.path.to_owned(),
                url: definition.url.to_owned(),
                dbtable: definition.dbtable.to_owned(),
                query: definition.query.to_owned(),
                auth: definition.auth.to_owned(),
                preprocessing: definition.preprocessing.to_owned(),
                event_timestamp_column: definition.event_timestamp_column.to_owned(),
                timestamp_format: definition.timestamp_format.to_owned(),
                type_: definition.source_type.to_owned(),
            }),
            version: 0,
            created_by: definition.created_by.to_owned(),
            created_on: Utc::now(),
        })
    }
    fn new_anchor(definition: &AnchorDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            qualified_name: definition.qualified_name.to_owned(),
            name: definition.name.to_owned(),
            tags: definition.tags.to_owned(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::Anchor,
            version: 0,
            created_by: definition.created_by.to_owned(),
            created_on: Utc::now(),
        })
    }
    fn new_anchor_feature(definition: &AnchorFeatureDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            qualified_name: definition.qualified_name.to_owned(),
            name: definition.name.to_owned(),
            tags: definition.tags.to_owned(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::AnchorFeature(AnchorFeatureAttributes {
                type_: definition.feature_type.to_owned(),
                transformation: definition.transformation.to_owned(),
                key: definition.key.to_owned(),
            }),
            version: 0,
            created_by: definition.created_by.to_owned(),
            created_on: Utc::now(),
        })
    }
    fn new_derived_feature(definition: &DerivedFeatureDef) -> Result<Self, RegistryError> {
        Ok(EntityProperty {
            guid: definition.id,
            qualified_name: definition.qualified_name.to_owned(),
            name: definition.name.to_owned(),
            tags: definition.tags.to_owned(),
            status: EntityStatus::Active,
            display_text: definition.name.to_owned(),
            labels: Default::default(),
            attributes: Attributes::DerivedFeature(DerivedFeatureAttributes {
                type_: definition.feature_type.to_owned(),
                transformation: definition.transformation.to_owned(),
                key: definition.key.to_owned(),
            }),
            version: 0,
            created_by: definition.created_by.to_owned(),
            created_on: Utc::now(),
        })
    }
    fn get_version(&self) -> u64 {
        self.version
    }
    fn set_version(&mut self, version: u64) {
        self.version = version;
    }
}

impl From<EntityProperty> for Entity<EntityProperty> {
    fn from(v: EntityProperty) -> Self {
        Entity::<EntityProperty> {
            id: v.guid,
            entity_type: match v.attributes {
                Attributes::AnchorFeature(_) => EntityType::AnchorFeature,
                Attributes::DerivedFeature(_) => EntityType::DerivedFeature,
                Attributes::Anchor => EntityType::Anchor,
                Attributes::Source(_) => EntityType::Source,
                Attributes::Project => EntityType::Project,
            },
            name: v.name.to_owned(),
            qualified_name: v.qualified_name.to_owned(),
            version: 0,
            properties: v,
        }
    }
}
