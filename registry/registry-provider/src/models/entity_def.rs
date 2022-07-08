use std::collections::{HashMap, HashSet};

use serde::{Serialize, Deserialize};
use uuid::Uuid;

use crate::{FeatureType, FeatureTransformation, TypedKey};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDef {
    pub id: Uuid,
    pub qualified_name: String,
    pub created_by: String,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDef {
    pub id: Uuid,
    pub name: String,
    pub qualified_name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    pub path: Option<String>,
    pub url: Option<String>,
    pub dbtable: Option<String>,
    pub query: Option<String>,
    pub auth: Option<String>,
    pub event_timestamp_column: Option<String>,
    pub timestamp_format: Option<String>,
    pub preprocessing: Option<String>,
    pub created_by: String,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorDef {
    pub id: Uuid,
    pub name: String,
    pub qualified_name: String,
    pub source_id: Uuid,
    pub created_by: String,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureDef {
    pub id: Uuid,
    pub name: String,
    pub qualified_name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub created_by: String,
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedFeatureDef {
    pub id: Uuid,
    pub name: String,
    pub qualified_name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub input_anchor_features: HashSet<Uuid>,
    pub input_derived_features: HashSet<Uuid>,
    pub created_by: String,
    pub tags: HashMap<String, String>,
}
