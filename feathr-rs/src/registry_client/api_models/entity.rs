use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Error;

use super::{EntityAttributes, Relationship};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntityType {
    #[serde(rename = "unknown")]
    Unknown,

    #[serde(rename = "feathr_workspace_v1")]
    Project,
    #[serde(rename = "feathr_source_v1")]
    Source,
    #[serde(rename = "feathr_anchor_v1")]
    Anchor,
    #[serde(rename = "feathr_anchor_feature_v1")]
    AnchorFeature,
    #[serde(rename = "feathr_derived_feature_v1")]
    DerivedFeature,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entity {
    pub guid: Uuid,
    pub name: String,
    pub qualified_name: String,
    #[serde(rename = "lastModifiedTS")]
    pub last_modified_ts: String,
    // #[serde(rename = "typeName")]
    // pub entity_type: EntityType,
    pub status: String,
    pub display_text: String,
    pub labels: Vec<String>,
    #[serde(flatten)]
    pub attributes: EntityAttributes,
}

impl Entity {
    pub fn get_entity_type(&self) -> EntityType {
        match &self.attributes {
            EntityAttributes::Project(_) => EntityType::Project,
            EntityAttributes::Source(_) => EntityType::Source,
            EntityAttributes::Anchor(_) => EntityType::Anchor,
            EntityAttributes::AnchorFeature(_) => EntityType::AnchorFeature,
            EntityAttributes::DerivedFeature(_) => EntityType::DerivedFeature,
        }
    }

    pub fn get_typed_key(&self) -> Result<Vec<crate::TypedKey>, Error> {
        if let Ok(r) = TryInto::<crate::feature::AnchorFeatureImpl>::try_into(self.to_owned()) {
            Ok(r.base.key)
        } else if let Ok(r) = TryInto::<crate::feature::DerivedFeatureImpl>::try_into(self.to_owned()) {
            Ok(r.base.key)
        } else {
            Err(Error::InvalidEntityType(self.name.to_owned(), self.get_entity_type().clone()))
        }
    }
}

impl TryInto<crate::project::FeathrProjectImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::project::FeathrProjectImpl, Self::Error> {
        // NOTE: returned project doesn't have owner, need to be set later
        match self.attributes {
            EntityAttributes::Project(attr) => (self.guid, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(self.guid.to_string(), self.get_entity_type()))
        }
    }
}

impl TryInto<crate::source::SourceImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::source::SourceImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::Source(attr) => (self.guid, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(self.guid.to_string(), self.get_entity_type()))
        }
    }
}

impl TryInto<crate::project::AnchorGroupImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::project::AnchorGroupImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::Anchor(attr) => (self.guid, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(self.guid.to_string(), self.get_entity_type()))
        }
    }
}

impl TryInto<crate::feature::AnchorFeatureImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::feature::AnchorFeatureImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::AnchorFeature(attr) => (self.guid, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(self.guid.to_string(), self.get_entity_type()))
        }
    }
}

impl TryInto<crate::feature::DerivedFeatureImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::feature::DerivedFeatureImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::DerivedFeature(attr) => (self.guid, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(self.guid.to_string(), self.get_entity_type()))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entities {
    pub entities: Vec<Entity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRef {
    guid: Uuid,
    type_name: String,
    unique_attributes: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityLineage {
    #[serde(rename = "guidEntityMap")]
    pub guid_entity_map: HashMap<Uuid, Entity>,
    pub relations: Vec<Relationship>,
}

