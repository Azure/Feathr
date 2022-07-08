use std::collections::HashMap;

use poem_openapi::{Enum, Object, Union};
use serde::{Deserialize, Serialize};

use super::{EntityRef, FeatureTransformation, FeatureType, TypedKey};

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum ValueType {
    #[serde(alias = "0")]
    UNSPECIFIED,
    #[serde(rename = "BOOLEAN", alias = "1")]
    #[oai(rename = "BOOLEAN")]
    BOOL,
    #[serde(rename = "INT", alias = "2")]
    #[oai(rename = "INT")]
    INT32,
    #[serde(rename = "LONG", alias = "3")]
    #[oai(rename = "LONG")]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum VectorType {
    TENSOR,
}

impl From<registry_provider::VectorType> for VectorType {
    fn from(_: registry_provider::VectorType) -> Self {
        VectorType::TENSOR
    }
}

impl Into<registry_provider::VectorType> for VectorType {
    fn into(self) -> registry_provider::VectorType {
        registry_provider::VectorType::TENSOR
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum TensorCategory {
    DENSE,
    SPARSE,
}

impl Default for TensorCategory {
    fn default() -> Self {
        Self::DENSE
    }
}

impl From<registry_provider::TensorCategory> for TensorCategory {
    fn from(v: registry_provider::TensorCategory) -> Self {
        match v {
            registry_provider::TensorCategory::DENSE => TensorCategory::DENSE,
            registry_provider::TensorCategory::SPARSE => TensorCategory::SPARSE,
        }
    }
}

impl Into<registry_provider::TensorCategory> for TensorCategory {
    fn into(self) -> registry_provider::TensorCategory {
        match self {
            TensorCategory::DENSE => registry_provider::TensorCategory::DENSE,
            TensorCategory::SPARSE => registry_provider::TensorCategory::SPARSE,
        }
    }
}

impl From<registry_provider::ValueType> for ValueType {
    fn from(v: registry_provider::ValueType) -> Self {
        match v {
            registry_provider::ValueType::UNSPECIFIED => Self::UNSPECIFIED,
            registry_provider::ValueType::BOOL => Self::BOOL,
            registry_provider::ValueType::INT32 => Self::INT32,
            registry_provider::ValueType::INT64 => Self::INT32,
            registry_provider::ValueType::FLOAT => Self::FLOAT,
            registry_provider::ValueType::DOUBLE => Self::DOUBLE,
            registry_provider::ValueType::STRING => Self::STRING,
            registry_provider::ValueType::BYTES => Self::BYTES,
        }
    }
}

impl Into<registry_provider::ValueType> for ValueType {
    fn into(self) -> registry_provider::ValueType {
        match self {
            ValueType::UNSPECIFIED => registry_provider::ValueType::UNSPECIFIED,
            ValueType::BOOL => registry_provider::ValueType::BOOL,
            ValueType::INT32 => registry_provider::ValueType::INT32,
            ValueType::INT64 => registry_provider::ValueType::INT64,
            ValueType::FLOAT => registry_provider::ValueType::FLOAT,
            ValueType::DOUBLE => registry_provider::ValueType::DOUBLE,
            ValueType::STRING => registry_provider::ValueType::STRING,
            ValueType::BYTES => registry_provider::ValueType::BYTES,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct ProjectAttributes {
    pub qualified_name: String,
    pub name: String,
    pub anchors: Vec<EntityRef>,
    pub sources: Vec<EntityRef>,
    pub anchor_features: Vec<EntityRef>,
    pub derived_features: Vec<EntityRef>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct SourceAttributes {
    pub qualified_name: String,
    pub name: String,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub dbtable: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub preprocessing: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub event_timestamp_column: Option<String>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub timestamp_format: Option<String>,
    #[oai(rename = "type")]
    pub type_: String,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct AnchorAttributes {
    pub qualified_name: String,
    pub name: String,
    pub features: Vec<EntityRef>,
    #[oai(skip_serializing_if = "Option::is_none")]
    pub source: Option<EntityRef>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct AnchorFeatureAttributes {
    pub qualified_name: String,
    pub name: String,
    #[oai(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
pub struct DerivedFeatureAttributes {
    #[oai(rename = "qualifiedName")]
    pub qualified_name: String,
    pub name: String,
    #[oai(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub input_anchor_features: Vec<EntityRef>,
    pub input_derived_features: Vec<EntityRef>,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Union)]
pub enum EntityAttributes {
    Project(ProjectAttributes),
    Source(SourceAttributes),
    Anchor(AnchorAttributes),
    AnchorFeature(AnchorFeatureAttributes),
    DerivedFeature(DerivedFeatureAttributes),
}

impl From<registry_provider::EntityProperty> for EntityAttributes {
    fn from(v: registry_provider::EntityProperty) -> Self {
        match v.attributes {
            registry_provider::Attributes::AnchorFeature(attr) => Self::AnchorFeature(AnchorFeatureAttributes {
                qualified_name: v.qualified_name,
                name: v.name,
                tags: v.tags,
                type_: attr.type_.into(),
                transformation: attr.transformation.into(),
                key: attr.key.into_iter().map(|e| e.into()).collect(),
            }),
            registry_provider::Attributes::DerivedFeature(attr) => Self::DerivedFeature(DerivedFeatureAttributes {
                qualified_name: v.qualified_name,
                name: v.name,
                tags: v.tags,
                type_: attr.type_.into(),
                transformation: attr.transformation.into(),
                key: attr.key.into_iter().map(|e| e.into()).collect(),
                input_anchor_features: Default::default(),
                input_derived_features: Default::default(),
            }),
            registry_provider::Attributes::Anchor => Self::Anchor(AnchorAttributes {
                qualified_name: v.qualified_name,
                name: v.name,
                tags: v.tags,
                features: Default::default(),
                source: None,
            }),
            registry_provider::Attributes::Source(attr) => Self::Source(SourceAttributes {
                qualified_name: v.qualified_name,
                name: v.name,
                tags: v.tags,
                path: attr.path,
                url: attr.url,
                dbtable: attr.dbtable,
                query: attr.query,
                auth: attr.auth,
                preprocessing: attr.preprocessing,
                event_timestamp_column: attr.event_timestamp_column,
                timestamp_format: attr.timestamp_format,
                type_: attr.type_,
                }),
            registry_provider::Attributes::Project => Self::Project(ProjectAttributes {
                qualified_name: v.qualified_name,
                name: v.name,
                tags: v.tags,
                anchors: Default::default(),
                sources: Default::default(),
                anchor_features: Default::default(),
                derived_features: Default::default(),
            }),
        }
    }
}
