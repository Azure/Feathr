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

fn default_version() -> u64 {
    1
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entity {
    pub guid: Uuid,
    // These 2 fields could be omitted
    #[serde(rename = "name", default)]
    _name: String,
    #[serde(rename = "qualifiedName", default)]
    _qualified_name: String,
    pub status: String,
    pub display_text: String,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(flatten)]
    pub attributes: EntityAttributes,
    #[serde(default = "default_version")]
    pub version: u64,
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
        } else if let Ok(r) =
            TryInto::<crate::feature::DerivedFeatureImpl>::try_into(self.to_owned())
        {
            Ok(r.base.key)
        } else {
            Err(Error::InvalidEntityType(
                self.get_name(),
                self.get_entity_type().clone(),
            ))
        }
    }

    pub fn get_name(&self) -> String {
        self.attributes.get_name()
    }

    pub fn get_qualified_name(&self) -> String {
        self.attributes.get_qualified_name()
    }
}

impl TryInto<crate::project::FeathrProjectImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::project::FeathrProjectImpl, Self::Error> {
        // NOTE: returned project doesn't have owner, need to be set later
        match self.attributes {
            EntityAttributes::Project(attr) => (self.guid, self.version, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(
                self.guid.to_string(),
                self.get_entity_type(),
            )),
        }
    }
}

impl TryInto<crate::source::SourceImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::source::SourceImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::Source(attr) => (self.guid, self.version, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(
                self.guid.to_string(),
                self.get_entity_type(),
            )),
        }
    }
}

impl TryInto<crate::project::AnchorGroupImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::project::AnchorGroupImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::Anchor(attr) => (self.guid, self.version, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(
                self.guid.to_string(),
                self.get_entity_type(),
            )),
        }
    }
}

impl TryInto<crate::feature::AnchorFeatureImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::feature::AnchorFeatureImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::AnchorFeature(attr) => (self.guid, self.version, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(
                self.guid.to_string(),
                self.get_entity_type(),
            )),
        }
    }
}

impl TryInto<crate::feature::DerivedFeatureImpl> for Entity {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::feature::DerivedFeatureImpl, Self::Error> {
        match self.attributes {
            EntityAttributes::DerivedFeature(attr) => (self.guid, self.version, attr).try_into(),
            _ => Err(Self::Error::InvalidEntityType(
                self.guid.to_string(),
                self.get_entity_type(),
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entities {
    pub entities: Vec<Entity>,
}


#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UniqueAttributes {
    qualified_name: String,
    #[serde(default = "default_version")]
    version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRef {
    guid: Uuid,
    type_name: String,
    unique_attributes: UniqueAttributes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityLineage {
    #[serde(rename = "guidEntityMap")]
    pub guid_entity_map: HashMap<Uuid, Entity>,
    pub relations: Vec<Relationship>,
}
