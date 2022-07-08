use chrono::Duration;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::utils::{dur_to_string, str_to_dur};

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    UNSPECIFIED,
    #[serde(rename = "BOOLEAN")]
    BOOL,
    #[serde(rename = "INT")]
    INT32,
    #[serde(rename = "LONG")]
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
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

#[allow(non_snake_case)]
impl FeatureType {
    pub const BOOLEAN: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::BOOL,
    };
    pub const INT32: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::INT32,
    };
    pub const INT64: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::INT64,
    };
    pub const FLOAT: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::FLOAT,
    };
    pub const DOUBLE: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::DOUBLE,
    };
    pub const STRING: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::STRING,
    };
    pub const BYTES: FeatureType = FeatureType {
        type_: VectorType::TENSOR,
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::BYTES,
    };

    pub fn INT32_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }

    pub fn INT64_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    pub fn FLOAT_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    pub fn DOUBLE_VECTOR() -> Self {
        FeatureType {
            type_: VectorType::TENSOR,
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
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

impl TypedKey {
    #[allow(non_snake_case)]
    pub fn DUMMY_KEY() -> TypedKey {
        TypedKey {
            key_column: "NOT_NEEDED".to_string(),
            key_column_type: ValueType::UNSPECIFIED,
            full_name: Some("feathr.dummy_typedkey".to_string()),
            description: Some("A dummy typed key for passthrough/request feature.".to_string()),
            key_column_alias: None,
        }
    }

    pub fn new(key_column: &str, key_column_type: ValueType) -> Self {
        Self {
            key_column: key_column.to_string(),
            key_column_type,
            full_name: None,
            description: None,
            key_column_alias: Some(key_column.to_string()),
        }
    }

    pub fn full_name(mut self, name: &str) -> Self {
        self.full_name = Some(name.to_owned());
        self
    }

    pub fn description(mut self, des: &str) -> Self {
        self.description = Some(des.to_owned());
        self
    }

    pub fn key_column_alias(mut self, alias: &str) -> Self {
        self.key_column_alias = Some(alias.to_owned());
        self
    }
}

/**
 * The built-in aggregation functions for LookupFeature
 */
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionDef {
    pub sql_expr: String,
}

fn ser_opt_dur<S>(d: &Option<Duration>, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match d.to_owned() {
        Some(d) => s.serialize_str(&dur_to_string(d)),
        None => s.serialize_none(),
    }
}

fn des_opt_dur<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;
    match s {
        Some(s) => match str_to_dur(&s) {
            Ok(d) => Ok(Some(d)),
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        },
        None => Ok(None),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Transformation {
    Expression {
        def: ExpressionDef,
    },
    WindowAgg {
        #[serde(rename = "def")]
        def_expr: String,
        #[serde(rename = "aggregation")]
        #[serde(skip_serializing_if = "Option::is_none", default)]
        agg_func: Option<Aggregation>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_opt_dur",
            deserialize_with = "des_opt_dur",
            default
        )]
        window: Option<Duration>,
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

impl Transformation {
    pub fn window_agg(
        def_expr: &str,
        agg_func: Aggregation,
        window: Duration,
    ) -> Result<Self, crate::Error> {
        Ok(Self::WindowAgg {
            def_expr: def_expr.to_string(),
            agg_func: Some(agg_func),
            window: Some(window),
            group_by: None,
            filter: None,
            limit: None,
        })
    }
}

impl<T> From<T> for Transformation
where
    T: AsRef<str>,
{
    fn from(s: T) -> Self {
        Transformation::Expression {
            def: ExpressionDef {
                sql_expr: String::from(s.as_ref()),
            },
        }
    }
}

impl From<&Transformation> for Transformation {
    fn from(t: &Transformation) -> Self {
        t.to_owned()
    }
}

/**
 * Derived feature uses a slightly different config format for transformation, no idea why
 */
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum DerivedTransformation {
    Expression {
        definition: String,
    },
    WindowAgg {
        #[serde(rename = "def")]
        def_expr: String,
        #[serde(rename = "aggregation")]
        #[serde(skip_serializing_if = "Option::is_none", default)]
        agg_func: Option<Aggregation>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_opt_dur",
            deserialize_with = "des_opt_dur",
            default
        )]
        window: Option<Duration>,
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

impl From<Transformation> for DerivedTransformation {
    fn from(t: Transformation) -> Self {
        match t {
            Transformation::Expression { def } => DerivedTransformation::Expression {
                definition: def.sql_expr,
            },
            Transformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            } => DerivedTransformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            },
            Transformation::Udf { name } => DerivedTransformation::Udf { name },
        }
    }
}

impl From<DerivedTransformation> for Transformation {
    fn from(t: DerivedTransformation) -> Self {
        match t {
            DerivedTransformation::Expression { definition } => Transformation::Expression {
                def: ExpressionDef {
                    sql_expr: definition,
                },
            },
            DerivedTransformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            } => Transformation::WindowAgg {
                def_expr,
                agg_func,
                window,
                group_by,
                filter,
                limit,
            },
            DerivedTransformation::Udf { name } => Transformation::Udf { name },
        }
    }
}
