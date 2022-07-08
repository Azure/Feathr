use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    feature::{AnchorFeatureImpl, DerivedFeatureImpl},
    project::AnchorGroupImpl,
    utils::{dur_to_string, str_to_dur},
    Error, SourceImpl,
};

mod attributes;
mod edge;
mod entity;

pub use attributes::*;
pub use edge::*;
pub use entity::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectDef {
    pub name: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceDef {
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub event_timestamp_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timestamp_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub preprocessing: Option<String>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl From<SourceImpl> for SourceDef {
    fn from(s: SourceImpl) -> Self {
        let (t, path) = match s.location {
            crate::SourceLocation::Hdfs { path } => ("hdfs".to_string(), path),
            crate::SourceLocation::InputContext => {
                ("PASSTHROUGH".to_string(), "PASSTHROUGH".to_string())
            }
            _ => todo!(),
        };
        Self {
            name: s.name,
            source_type: t,
            path,
            event_timestamp_column: s.time_window_parameters.clone().map(|t| t.timestamp_column),
            timestamp_format: s.time_window_parameters.map(|t| t.timestamp_column_format),
            preprocessing: s.preprocessing,
            tags: s.registry_tags,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorDef {
    pub name: String,
    pub source_id: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl From<AnchorGroupImpl> for AnchorDef {
    fn from(g: AnchorGroupImpl) -> Self {
        Self {
            name: g.name,
            source_id: g.source.inner.id.to_string(),
            tags: g.registry_tags,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureType {
    #[serde(rename = "type")]
    pub type_: VectorType,
    #[serde(default)]
    pub tensor_category: TensorCategory,
    #[serde(default)]
    pub dimension_type: Vec<ValueType>,
    pub val_type: ValueType,
}

impl Into<crate::FeatureType> for FeatureType {
    fn into(self) -> crate::FeatureType {
        crate::FeatureType {
            type_: self.type_.into(),
            tensor_category: self.tensor_category.into(),
            dimension_type: self.dimension_type.into_iter().map(|e| e.into()).collect(),
            val_type: self.val_type.into(),
        }
    }
}

impl From<crate::FeatureType> for FeatureType {
    fn from(v: crate::FeatureType) -> Self {
        Self {
            type_: v.type_.into(),
            tensor_category: v.tensor_category.into(),
            dimension_type: v.dimension_type.into_iter().map(|e| e.into()).collect(),
            val_type: v.val_type.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypedKey {
    pub key_column: String,
    pub key_column_type: ValueType,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub full_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub key_column_alias: Option<String>,
}

impl From<crate::TypedKey> for TypedKey {
    fn from(v: crate::TypedKey) -> Self {
        Self {
            key_column: v.key_column,
            key_column_type: v.key_column_type.into(),
            full_name: v.full_name,
            description: v.description,
            key_column_alias: v.key_column_alias,
        }
    }
}

impl Into<crate::TypedKey> for TypedKey {
    fn into(self) -> crate::TypedKey {
        crate::TypedKey {
            key_column: self.key_column,
            key_column_type: self.key_column_type.into(),
            full_name: self.full_name,
            description: self.description,
            key_column_alias: self.key_column_alias,
        }
    }
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl From<crate::Aggregation> for Aggregation {
    fn from(v: crate::Aggregation) -> Self {
        match v {
            crate::Aggregation::NOP => Aggregation::NOP,
            crate::Aggregation::AVG => Aggregation::AVG,
            crate::Aggregation::MAX => Aggregation::MAX,
            crate::Aggregation::MIN => Aggregation::MIN,
            crate::Aggregation::SUM => Aggregation::SUM,
            crate::Aggregation::UNION => Aggregation::UNION,
            crate::Aggregation::ELEMENTWISE_AVG => Aggregation::ELEMENTWISE_AVG,
            crate::Aggregation::ELEMENTWISE_MIN => Aggregation::ELEMENTWISE_MIN,
            crate::Aggregation::ELEMENTWISE_MAX => Aggregation::ELEMENTWISE_MAX,
            crate::Aggregation::ELEMENTWISE_SUM => Aggregation::ELEMENTWISE_SUM,
            crate::Aggregation::LATEST => Aggregation::LATEST,
        }
    }
}

impl Into<crate::Aggregation> for Aggregation {
    fn into(self) -> crate::Aggregation {
        match self {
            Aggregation::NOP => crate::Aggregation::NOP,
            Aggregation::AVG => crate::Aggregation::AVG,
            Aggregation::MAX => crate::Aggregation::MAX,
            Aggregation::MIN => crate::Aggregation::MIN,
            Aggregation::SUM => crate::Aggregation::SUM,
            Aggregation::UNION => crate::Aggregation::UNION,
            Aggregation::ELEMENTWISE_AVG => crate::Aggregation::ELEMENTWISE_AVG,
            Aggregation::ELEMENTWISE_MIN => crate::Aggregation::ELEMENTWISE_MIN,
            Aggregation::ELEMENTWISE_MAX => crate::Aggregation::ELEMENTWISE_MAX,
            Aggregation::ELEMENTWISE_SUM => crate::Aggregation::ELEMENTWISE_SUM,
            Aggregation::LATEST => crate::Aggregation::LATEST,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureTransformation {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    def_expr: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none", default)]
    transform_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    name: Option<String>,
}

impl TryInto<crate::Transformation> for FeatureTransformation {
    type Error = crate::Error;

    fn try_into(self) -> Result<crate::Transformation, Self::Error> {
        Ok(match self.transform_expr {
            Some(s) => crate::Transformation::Expression {
                def: crate::ExpressionDef { sql_expr: s },
            },
            None => match self.name {
                Some(s) => crate::Transformation::Udf { name: s },
                None => match self.def_expr {
                    Some(s) => crate::Transformation::WindowAgg {
                        def_expr: s,
                        agg_func: self.agg_func.map(|a| a.into()),
                        window: match self.window {
                            Some(s) => Some(str_to_dur(&s)?),
                            None => None,
                        },
                        group_by: self.group_by,
                        filter: self.filter,
                        limit: self.limit,
                    },
                    None => {
                        return Err(Error::MissingTransformation(
                            "Invalid feature transformation".to_string(),
                        ))
                    }
                },
            },
        })
    }
}

impl From<crate::Transformation> for FeatureTransformation {
    fn from(v: crate::Transformation) -> Self {
        match v {
            crate::Transformation::Expression {
                def: crate::ExpressionDef { sql_expr: s },
            } => Self {
                transform_expr: Some(s),
                ..Default::default()
            },
            crate::Transformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            } => Self {
                def_expr: Some(def_expr),
                agg_func: agg_func.map(|a| a.into()),
                window: window.map(dur_to_string),
                group_by,
                filter,
                limit,
                ..Default::default()
            },
            crate::Transformation::Udf { name } => Self {
                name: Some(name),
                ..Default::default()
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnchorFeatureDef {
    pub name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl From<AnchorFeatureImpl> for AnchorFeatureDef {
    fn from(f: AnchorFeatureImpl) -> Self {
        Self {
            name: f.base.name,
            feature_type: f.base.feature_type.into(),
            transformation: f.transform.into(),
            key: f.base.key.into_iter().map(Into::into).collect(),
            tags: f.base.registry_tags,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedFeatureDef {
    pub name: String,
    pub feature_type: FeatureType,
    pub transformation: FeatureTransformation,
    pub key: Vec<TypedKey>,
    pub input_anchor_features: Vec<Uuid>,
    pub input_derived_features: Vec<Uuid>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

impl From<DerivedFeatureImpl> for DerivedFeatureDef {
    fn from(f: DerivedFeatureImpl) -> Self {
        Self {
            name: f.base.name,
            feature_type: f.base.feature_type.into(),
            transformation: Into::<crate::Transformation>::into(f.transform).into(),
            key: f.base.key.into_iter().map(Into::into).collect(),
            input_anchor_features: f
                .inputs
                .iter()
                .filter(|(_, f)| f.is_anchor_feature)
                .map(|(_, f)| f.id)
                .collect(),
            input_derived_features: f
                .inputs
                .iter()
                .filter(|(_, f)| !f.is_anchor_feature)
                .map(|(_, f)| f.id)
                .collect(),
            tags: f.base.registry_tags,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreationResponse {
    pub guid: Uuid,
}
