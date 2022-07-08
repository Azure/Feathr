use std::fmt::Debug;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

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

impl Default for ValueType {
    fn default() -> Self {
        ValueType::UNSPECIFIED
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum VectorType {
    TENSOR,
}

impl Default for VectorType {
    fn default() -> Self {
        VectorType::TENSOR
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TensorCategory {
    DENSE,
    SPARSE,
}

impl Default for TensorCategory {
    fn default() -> Self {
        TensorCategory::DENSE
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureType {
    #[serde(rename = "type")]
    pub type_: VectorType,
    pub tensor_category: TensorCategory,
    pub dimension_type: Vec<ValueType>,
    pub val_type: ValueType,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TypedKey {
    pub key_column: String,
    pub key_column_type: ValueType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_column_alias: Option<String>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregation {
    // No operation
    NOP,
    // Average
    AVG,
    MAX,
    MIN,
    SUM,
    UNION,
    // Element-wise average, typically used in array type value, i.e. 1d dense tensor
    ELEMENTWISE_AVG,
    ELEMENTWISE_MIN,
    ELEMENTWISE_MAX,
    ELEMENTWISE_SUM,
    // Pick the latest value according to its timestamp
    LATEST,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FeatureTransformation {
    Expression {
        transform_expr: String,
    },
    WindowAgg {
        def_expr: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        agg_func: Option<Aggregation>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        window: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        group_by: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        filter: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        limit: Option<u64>,
    },
    Udf {
        name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureAttributes {
    #[serde(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedFeatureAttributes {
    #[serde(rename = "type")]
    pub type_: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceAttributes {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dbtable: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub auth: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub preprocessing: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub event_timestamp_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timestamp_format: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "typeName", content = "attributes")]
pub enum Attributes {
    #[serde(rename = "feathr_anchor_feature_v1")]
    AnchorFeature(AnchorFeatureAttributes),
    #[serde(rename = "feathr_derived_feature_v1")]
    DerivedFeature(DerivedFeatureAttributes),
    #[serde(rename = "feathr_anchor_v1")]
    Anchor,
    #[serde(rename = "feathr_source_v1")]
    Source(SourceAttributes),
    #[serde(rename = "feathr_workspace_v1")]
    Project,
}
