use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{feature::FeatureBase, SourceImpl, Transformation};

use super::{EntityRef, FeatureTransformation, FeatureType, TypedKey};

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    #[serde(alias = "0")]
    UNSPECIFIED,
    #[serde(rename = "BOOLEAN", alias = "1")]
    BOOL,
    #[serde(rename = "INT", alias = "2")]
    INT32,
    #[serde(rename = "LONG", alias = "3")]
    INT64,
    #[serde(alias = "4")]
    FLOAT,
    #[serde(alias = "5")]
    DOUBLE,
    #[serde(alias = "6")]
    STRING,
    #[serde(alias = "7")]
    BYTES,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorType {
    TENSOR,
}

impl From<crate::VectorType> for VectorType {
    fn from(_: crate::VectorType) -> Self {
        VectorType::TENSOR
    }
}

impl Into<crate::VectorType> for VectorType {
    fn into(self) -> crate::VectorType {
        crate::VectorType::TENSOR
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TensorCategory {
    DENSE,
    SPARSE,
}

impl Default for TensorCategory {
    fn default() -> Self {
        Self::DENSE
    }
}

impl From<crate::TensorCategory> for TensorCategory {
    fn from(v: crate::TensorCategory) -> Self {
        match v {
            crate::TensorCategory::DENSE => TensorCategory::DENSE,
            crate::TensorCategory::SPARSE => TensorCategory::SPARSE,
        }
    }
}

impl Into<crate::TensorCategory> for TensorCategory {
    fn into(self) -> crate::TensorCategory {
        match self {
            TensorCategory::DENSE => crate::TensorCategory::DENSE,
            TensorCategory::SPARSE => crate::TensorCategory::SPARSE,
        }
    }
}

impl From<crate::ValueType> for ValueType {
    fn from(v: crate::ValueType) -> Self {
        match v {
            crate::ValueType::UNSPECIFIED => Self::UNSPECIFIED,
            crate::ValueType::BOOL => Self::BOOL,
            crate::ValueType::INT32 => Self::INT32,
            crate::ValueType::INT64 => Self::INT32,
            crate::ValueType::FLOAT => Self::FLOAT,
            crate::ValueType::DOUBLE => Self::DOUBLE,
            crate::ValueType::STRING => Self::STRING,
            crate::ValueType::BYTES => Self::BYTES,
        }
    }
}

impl Into<crate::ValueType> for ValueType {
    fn into(self) -> crate::ValueType {
        match self {
            ValueType::UNSPECIFIED => crate::ValueType::UNSPECIFIED,
            ValueType::BOOL => crate::ValueType::BOOL,
            ValueType::INT32 => crate::ValueType::INT32,
            ValueType::INT64 => crate::ValueType::INT64,
            ValueType::FLOAT => crate::ValueType::FLOAT,
            ValueType::DOUBLE => crate::ValueType::DOUBLE,
            ValueType::STRING => crate::ValueType::STRING,
            ValueType::BYTES => crate::ValueType::BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectAttributes {
    pub qualified_name: String,
    pub name: String,
    pub anchors: Vec<EntityRef>,
    pub sources: Vec<EntityRef>,
    pub anchor_features: Vec<EntityRef>,
    pub derived_features: Vec<EntityRef>,
    pub tags: HashMap<String, String>,
}

impl TryInto<crate::project::FeathrProjectImpl> for (Uuid, ProjectAttributes) {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::project::FeathrProjectImpl, Self::Error> {
        // Generated FeathrProjectImpl only contains base attributes, without *owner* and contained sources/anchors, etc.
        Ok(crate::project::FeathrProjectImpl {
            owner: None,
            id: self.0,
            name: self.1.name,
            anchor_groups: Default::default(),
            derivations: Default::default(),
            anchor_features: Default::default(),
            anchor_map: Default::default(),
            sources: Default::default(),
            registry_tags: self.1.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceAttributes {
    pub qualified_name: String,
    pub name: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preprocessing: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_timestamp_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
    #[serde(rename = "type")]
    pub type_: String,
    pub tags: HashMap<String, String>,
}

impl TryInto<crate::source::SourceImpl> for (Uuid, SourceAttributes) {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::source::SourceImpl, Self::Error> {
        Ok(if self.1.name == "PASSTHROUGH" {
            SourceImpl {
                id: self.0,
                name: self.1.name,
                location: crate::SourceLocation::InputContext,
                time_window_parameters: None,
                preprocessing: None,
                registry_tags: Default::default(),
            }
        } else {
            SourceImpl {
                id: self.0,
                name: self.1.name,
                location: crate::SourceLocation::Hdfs { path: self.1.path },
                time_window_parameters: self.1.event_timestamp_column.map(|c| {
                    crate::TimeWindowParameters {
                        timestamp_column: c,
                        timestamp_column_format: self.1.timestamp_format.unwrap_or_default(),
                    }
                }),
                preprocessing: self.1.preprocessing,
                registry_tags: self.1.tags,
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorAttributes {
    pub qualified_name: String,
    pub name: String,
    pub features: Vec<EntityRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<EntityRef>,
    pub tags: HashMap<String, String>,
}

impl TryInto<crate::project::AnchorGroupImpl> for (Uuid, AnchorAttributes) {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::project::AnchorGroupImpl, Self::Error> {
        // Generated AnchorGroupImpl only contains base attributes, without contained features.
        Ok(crate::project::AnchorGroupImpl {
            id: self.0,
            name: self.1.name,
            source: Default::default(),
            registry_tags: self.1.tags,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureAttributes {
    pub qualified_name: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub tags: HashMap<String, String>,
}

impl TryInto<crate::feature::AnchorFeatureImpl> for (Uuid, AnchorFeatureAttributes) {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::feature::AnchorFeatureImpl, Self::Error> {
        let key: Vec<crate::TypedKey> = self.1.key.into_iter().map(|k| k.into()).collect();
        let key_alias = key
            .iter()
            .map(|k| {
                k.key_column_alias
                    .as_ref()
                    .unwrap_or(&k.key_column)
                    .to_owned()
            })
            .collect();
        Ok(crate::feature::AnchorFeatureImpl {
            base: FeatureBase {
                id: self.0,
                name: self.1.name.clone(),
                feature_type: self.1.type_.into(),
                key,
                feature_alias: self.1.name,
                registry_tags: self.1.tags,
            },
            key_alias,
            transform: self.1.transformation.try_into()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedFeatureAttributes {
    pub qualified_name: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub input_anchor_features: Vec<EntityRef>,
    pub input_derived_features: Vec<EntityRef>,
    pub tags: HashMap<String, String>,
}

impl TryInto<crate::feature::DerivedFeatureImpl> for (Uuid, DerivedFeatureAttributes) {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::feature::DerivedFeatureImpl, Self::Error> {
        let key: Vec<crate::TypedKey> = self.1.key.into_iter().map(|k| k.into()).collect();
        let key_alias = key
            .iter()
            .map(|k| {
                k.key_column_alias
                    .as_ref()
                    .unwrap_or(&k.key_column)
                    .to_owned()
            })
            .collect();
        let t: Transformation = self.1.transformation.try_into()?;
        Ok(crate::feature::DerivedFeatureImpl {
            base: FeatureBase {
                id: self.0,
                name: self.1.name.clone(),
                feature_type: self.1.type_.into(),
                key,
                feature_alias: self.1.name,
                registry_tags: self.1.tags,
            },
            key_alias,
            transform: t.into(),
            inputs: Default::default(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "typeName", content = "attributes")]
pub enum EntityAttributes {
    #[serde(rename = "feathr_workspace_v1")]
    Project(ProjectAttributes),
    #[serde(rename = "feathr_source_v1")]
    Source(SourceAttributes),
    #[serde(rename = "feathr_anchor_v1")]
    Anchor(AnchorAttributes),
    #[serde(rename = "feathr_anchor_feature_v1")]
    AnchorFeature(AnchorFeatureAttributes),
    #[serde(rename = "feathr_derived_feature_v1")]
    DerivedFeature(DerivedFeatureAttributes),
}
