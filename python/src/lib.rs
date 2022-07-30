use std::collections::HashMap;
use std::fmt::Debug;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use feathr::Feature;
use futures::future::join_all;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::types::{PyDateAccess, PyDateTime, PyList, PyTimeAccess, PyTuple};
use pyo3::{exceptions::PyTypeError, prelude::*, pyclass::CompareOp};
use utils::{block_on, cancelable_wait, value_to_py};

mod utils;

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ValueType {
    UNSPECIFIED,
    BOOL,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    STRING,
    BYTES,
}

#[pymethods]
impl ValueType {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::ValueType> for ValueType {
    fn from(v: feathr::ValueType) -> Self {
        match v {
            feathr::ValueType::UNSPECIFIED => ValueType::UNSPECIFIED,
            feathr::ValueType::BOOL => ValueType::BOOL,
            feathr::ValueType::INT32 => ValueType::INT32,
            feathr::ValueType::INT64 => ValueType::INT64,
            feathr::ValueType::FLOAT => ValueType::FLOAT,
            feathr::ValueType::DOUBLE => ValueType::DOUBLE,
            feathr::ValueType::STRING => ValueType::STRING,
            feathr::ValueType::BYTES => ValueType::BYTES,
        }
    }
}

impl Into<feathr::ValueType> for ValueType {
    fn into(self) -> feathr::ValueType {
        match self {
            ValueType::UNSPECIFIED => feathr::ValueType::UNSPECIFIED,
            ValueType::BOOL => feathr::ValueType::BOOL,
            ValueType::INT32 => feathr::ValueType::INT32,
            ValueType::INT64 => feathr::ValueType::INT64,
            ValueType::FLOAT => feathr::ValueType::FLOAT,
            ValueType::DOUBLE => feathr::ValueType::DOUBLE,
            ValueType::STRING => feathr::ValueType::STRING,
            ValueType::BYTES => feathr::ValueType::BYTES,
        }
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum VectorType {
    TENSOR,
}

#[pymethods]
impl VectorType {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::VectorType> for VectorType {
    fn from(_: feathr::VectorType) -> Self {
        VectorType::TENSOR
    }
}

impl Into<feathr::VectorType> for VectorType {
    fn into(self) -> feathr::VectorType {
        feathr::VectorType::TENSOR
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum TensorCategory {
    DENSE,
    SPARSE,
}

#[pymethods]
impl TensorCategory {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::TensorCategory> for TensorCategory {
    fn from(v: feathr::TensorCategory) -> Self {
        match v {
            feathr::TensorCategory::DENSE => TensorCategory::DENSE,
            feathr::TensorCategory::SPARSE => TensorCategory::SPARSE,
        }
    }
}

impl Into<feathr::TensorCategory> for TensorCategory {
    fn into(self) -> feathr::TensorCategory {
        match self {
            TensorCategory::DENSE => feathr::TensorCategory::DENSE,
            TensorCategory::SPARSE => feathr::TensorCategory::SPARSE,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FeatureType {
    #[pyo3(get)]
    tensor_category: TensorCategory,
    #[pyo3(get)]
    dimension_type: Vec<ValueType>,
    #[pyo3(get)]
    val_type: ValueType,
}

#[allow(non_snake_case)]
#[pymethods]
impl FeatureType {
    #[classattr]
    pub const BOOLEAN: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::BOOL,
    };
    #[classattr]
    pub const INT32: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::INT32,
    };
    #[classattr]
    pub const INT64: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::INT64,
    };
    #[classattr]
    pub const FLOAT: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::FLOAT,
    };
    #[classattr]
    pub const DOUBLE: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::DOUBLE,
    };
    #[classattr]
    pub const STRING: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::STRING,
    };
    #[classattr]
    pub const BYTES: FeatureType = FeatureType {
        tensor_category: TensorCategory::DENSE,
        dimension_type: vec![],
        val_type: ValueType::BYTES,
    };
    #[classattr]
    pub fn INT32_VECTOR() -> Self {
        FeatureType {
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    #[classattr]
    pub fn INT64_VECTOR() -> Self {
        FeatureType {
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    #[classattr]
    pub fn FLOAT_VECTOR() -> Self {
        FeatureType {
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }
    #[classattr]
    pub fn DOUBLE_VECTOR() -> Self {
        FeatureType {
            tensor_category: TensorCategory::DENSE,
            dimension_type: vec![ValueType::INT32],
            val_type: ValueType::BOOL,
        }
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::FeatureType> for FeatureType {
    fn from(v: feathr::FeatureType) -> Self {
        Self {
            tensor_category: v.tensor_category.into(),
            dimension_type: v.dimension_type.into_iter().map(|t| t.into()).collect(),
            val_type: v.val_type.into(),
        }
    }
}

impl Into<feathr::FeatureType> for FeatureType {
    fn into(self) -> feathr::FeatureType {
        feathr::FeatureType {
            type_: feathr::VectorType::TENSOR,
            tensor_category: self.tensor_category.into(),
            dimension_type: self.dimension_type.into_iter().map(|t| t.into()).collect(),
            val_type: self.val_type.into(),
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TypedKey {
    #[pyo3(get)]
    key_column: String,
    #[pyo3(get)]
    key_column_type: ValueType,
    #[pyo3(get)]
    full_name: Option<String>,
    #[pyo3(get)]
    description: Option<String>,
    #[pyo3(get)]
    key_column_alias: Option<String>,
}

#[allow(non_snake_case)]
#[pymethods]
impl TypedKey {
    #[new]
    #[args(full_name = "None", description = "None")]
    fn new(
        key_column: &str,
        key_column_type: ValueType,
        full_name: Option<String>,
        description: Option<String>,
    ) -> Self {
        Self {
            key_column: key_column.to_string(),
            key_column_type,
            full_name,
            description,
            key_column_alias: Some(key_column.to_string()),
        }
    }

    #[classattr]
    fn DUMMY_KEY() -> TypedKey {
        TypedKey {
            key_column: "NOT_NEEDED".to_string(),
            key_column_type: ValueType::UNSPECIFIED,
            full_name: Some("feathr.dummy_typedkey".to_string()),
            description: Some("A dummy typed key for passthrough/request feature.".to_string()),
            key_column_alias: None,
        }
    }

    fn as_key(&self, key_column_alias: &str) -> Self {
        let mut ret = self.clone();
        ret.key_column_alias = Some(key_column_alias.to_string());
        ret
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::TypedKey> for TypedKey {
    fn from(v: feathr::TypedKey) -> Self {
        Self {
            key_column: v.key_column,
            key_column_type: v.key_column_type.into(),
            full_name: v.full_name,
            description: v.description,
            key_column_alias: v.key_column_alias,
        }
    }
}

impl Into<feathr::TypedKey> for TypedKey {
    fn into(self) -> feathr::TypedKey {
        feathr::TypedKey {
            key_column: self.key_column,
            key_column_type: self.key_column_type.into(),
            full_name: self.full_name,
            description: self.description,
            key_column_alias: self.key_column_alias,
        }
    }
}

#[allow(non_camel_case_types)]
#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Aggregation {
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

#[pymethods]
impl Aggregation {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::Aggregation> for Aggregation {
    fn from(v: feathr::Aggregation) -> Self {
        match v {
            feathr::Aggregation::NOP => Aggregation::NOP,
            feathr::Aggregation::AVG => Aggregation::AVG,
            feathr::Aggregation::MAX => Aggregation::MAX,
            feathr::Aggregation::MIN => Aggregation::MIN,
            feathr::Aggregation::SUM => Aggregation::SUM,
            feathr::Aggregation::UNION => Aggregation::UNION,
            feathr::Aggregation::ELEMENTWISE_AVG => Aggregation::ELEMENTWISE_AVG,
            feathr::Aggregation::ELEMENTWISE_MIN => Aggregation::ELEMENTWISE_MIN,
            feathr::Aggregation::ELEMENTWISE_MAX => Aggregation::ELEMENTWISE_MAX,
            feathr::Aggregation::ELEMENTWISE_SUM => Aggregation::ELEMENTWISE_SUM,
            feathr::Aggregation::LATEST => Aggregation::LATEST,
        }
    }
}

impl Into<feathr::Aggregation> for Aggregation {
    fn into(self) -> feathr::Aggregation {
        match self {
            Aggregation::NOP => feathr::Aggregation::NOP,
            Aggregation::AVG => feathr::Aggregation::AVG,
            Aggregation::MAX => feathr::Aggregation::MAX,
            Aggregation::MIN => feathr::Aggregation::MIN,
            Aggregation::SUM => feathr::Aggregation::SUM,
            Aggregation::UNION => feathr::Aggregation::UNION,
            Aggregation::ELEMENTWISE_AVG => feathr::Aggregation::ELEMENTWISE_AVG,
            Aggregation::ELEMENTWISE_MIN => feathr::Aggregation::ELEMENTWISE_MIN,
            Aggregation::ELEMENTWISE_MAX => feathr::Aggregation::ELEMENTWISE_MAX,
            Aggregation::ELEMENTWISE_SUM => feathr::Aggregation::ELEMENTWISE_SUM,
            Aggregation::LATEST => feathr::Aggregation::LATEST,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, PartialEq, Eq)]
struct Transformation(feathr::Transformation);

#[pymethods]
impl Transformation {
    #[new]
    fn from_str(s: &str) -> Self {
        Self(feathr::Transformation::from(s))
    }

    #[staticmethod]
    fn window_agg(def_expr: &str, agg_func: Aggregation, window: &str) -> PyResult<Self> {
        Ok(Self(
            feathr::Transformation::window_agg(
                def_expr,
                agg_func.into(),
                utils::str_to_dur(window)?,
            )
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
        ))
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

impl From<feathr::Transformation> for Transformation {
    fn from(v: feathr::Transformation) -> Self {
        Self(v)
    }
}

impl Into<feathr::Transformation> for Transformation {
    fn into(self) -> feathr::Transformation {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Debug, Eq, PartialEq)]
struct DataLocation(feathr::DataLocation);
#[pymethods]
impl DataLocation {
    #[new]
    fn new<'p>(py: Python<'p>, value: &PyAny) -> PyResult<Self> {
        if let Ok(s) = value.extract::<String>() {
            Ok(DataLocation(
                s.parse()
                    .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
            ))
        } else {
            let dumps: Py<PyAny> = py.import("json")?.getattr("dumps")?.into();
            let ret = dumps.call1(py, PyTuple::new(py, &[value]))?;
            if let Ok(s) = ret.extract::<String>(py) {
                Ok(DataLocation(
                    s.parse()
                        .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
                ))
            } else {
                Err(PyValueError::new_err("Invalid data location"))
            }
        }
    }

    #[getter]
    fn get_type(&self) -> String {
        self.0.get_type()
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }

    fn __repr__(&self) -> String {
        format!("DataLocation(type='{}')", self.get_type(),)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

#[pyclass]
#[derive(Clone, Debug, Eq, PartialEq)]
struct Source(feathr::Source);

#[pymethods]
impl Source {
    #[getter]
    fn get_id(&self) -> String {
        self.0.get_id().to_string()
    }

    #[getter]
    fn get_version(&self) -> u64 {
        self.0.get_version()
    }

    #[getter]
    fn get_name(&self) -> String {
        self.0.get_name()
    }

    #[getter]
    fn get_type(&self) -> String {
        self.0.get_type()
    }

    #[getter]
    fn get_location(&self) -> DataLocation {
        DataLocation(self.0.get_location())
    }

    #[getter]
    pub fn get_secret_keys(&self) -> Vec<String> {
        self.0.get_secret_keys()
    }

    #[getter]
    pub fn get_preprocessing(&self) -> Option<String> {
        self.0.get_preprocessing()
    }

    fn __repr__(&self) -> String {
        format!(
            "Source(id='{}', name='{}', version={})",
            self.get_id(),
            self.get_name(),
            self.get_version()
        )
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

impl From<feathr::Source> for Source {
    fn from(v: feathr::Source) -> Self {
        Self(v)
    }
}

impl Into<feathr::Source> for Source {
    fn into(self) -> feathr::Source {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum JdbcSourceAuth {
    Anonymous,
    Userpass,
    Token,
}

#[pymethods]
impl JdbcSourceAuth {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }

    fn __hash__(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl From<feathr::JdbcSourceAuth> for JdbcSourceAuth {
    fn from(v: feathr::JdbcSourceAuth) -> Self {
        match v {
            feathr::JdbcSourceAuth::Anonymous => JdbcSourceAuth::Anonymous,
            feathr::JdbcSourceAuth::Userpass => JdbcSourceAuth::Userpass,
            feathr::JdbcSourceAuth::Token => JdbcSourceAuth::Token,
        }
    }
}

impl Into<feathr::JdbcSourceAuth> for JdbcSourceAuth {
    fn into(self) -> feathr::JdbcSourceAuth {
        match self {
            JdbcSourceAuth::Anonymous => feathr::JdbcSourceAuth::Anonymous,
            JdbcSourceAuth::Userpass => feathr::JdbcSourceAuth::Userpass,
            JdbcSourceAuth::Token => feathr::JdbcSourceAuth::Token,
        }
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateTimeResolution {
    Daily,
    Hourly,
}

#[pymethods]
impl DateTimeResolution {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

impl Into<feathr::DateTimeResolution> for DateTimeResolution {
    fn into(self) -> feathr::DateTimeResolution {
        match self {
            DateTimeResolution::Daily => feathr::DateTimeResolution::Daily,
            DateTimeResolution::Hourly => feathr::DateTimeResolution::Hourly,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RedisSink(feathr::RedisSink);

#[pymethods]
impl RedisSink {
    #[new]
    #[args(streaming = "false", streaming_timeout = "None")]
    fn new(table_name: &str, streaming: bool, streaming_timeout: Option<i64>) -> Self {
        Self(feathr::RedisSink {
            table_name: table_name.to_string(),
            streaming,
            streaming_timeout: streaming_timeout.map(|i| Duration::seconds(i)),
        })
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct CosmosDbSink(feathr::GenericSink);

#[pymethods]
impl CosmosDbSink {
    #[new]
    #[args(streaming = "false", streaming_timeout = "None")]
    fn new(
        name: &str,
        endpoint: &str,
        database: &str,
        collection: &str,
        streaming: bool,
        streaming_timeout: Option<i64>,
    ) -> Self {
        let mut options: HashMap<String, String> = HashMap::new();
        options.insert(
            "spark__cosmos__accountEndpoint".to_string(),
            endpoint.to_string(),
        );
        options.insert("spark__cosmos__database".to_string(), database.to_string());
        options.insert(
            "spark__cosmos__container".to_string(),
            collection.to_string(),
        );
        options.insert(
            "spark__cosmos__accountKey".to_string(),
            format!("${{{}_KEY}}", name),
        );
        let location = feathr::DataLocation::Generic {
            _type: "generic".to_string(),
            format: "cosmos.oltp".to_string(),
            mode: Some("APPEND".to_string()),
            options,
        };
        Self(feathr::GenericSink {
            location,
            streaming,
            streaming_timeout: streaming_timeout.map(|i| Duration::seconds(i)),
        })
    }

    #[getter]
    fn get_location(&self) -> DataLocation {
        DataLocation(self.0.location.clone())
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct ObservationSettings(feathr::ObservationSettings);

#[pymethods]
impl ObservationSettings {
    #[new]
    #[args(timestamp_column = "None", format = "None")]
    fn new(
        observation_path: &str,
        timestamp_column: Option<&str>,
        format: Option<&str>,
    ) -> PyResult<Self> {
        if let Some(timestamp_column) = timestamp_column {
            if let Some(format) = format {
                Ok(Self(
                    feathr::ObservationSettings::new(observation_path, timestamp_column, format)
                        .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
                ))
            } else {
                Ok(Self(
                    feathr::ObservationSettings::new(observation_path, timestamp_column, "epoch")
                        .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
                ))
            }
        } else {
            Ok(Self(
                feathr::ObservationSettings::from_path(observation_path)
                    .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
            ))
        }
    }

    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct FeatureQuery(feathr::FeatureQuery);

#[pymethods]
impl FeatureQuery {
    #[new]
    fn new(names: &PyList, keys: Vec<TypedKey>) -> Self {
        let keys: Vec<feathr::TypedKey> = keys.into_iter().map(|k| k.into()).collect();
        let keys: Vec<&feathr::TypedKey> = keys.iter().map(|k| k).collect();
        let mut n: Vec<String> = vec![];
        for name in names.into_iter() {
            if let Ok(name) = name.extract::<String>() {
                n.push(name);
            } else if let Ok(feature) = name.extract::<AnchorFeature>() {
                n.push(feature.0.to_string())
            } else if let Ok(feature) = name.extract::<DerivedFeature>() {
                n.push(feature.0.to_string())
            }
        }
        Self(feathr::FeatureQuery::new(&n, &keys))
    }

    #[staticmethod]
    fn by_name(names: Vec<&str>) -> Self {
        Self(feathr::FeatureQuery::by_name(&names))
    }
}

#[pyclass]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Starting,
    Running,
    Success,
    Failed,
}

#[pymethods]
impl JobStatus {
    fn __repr__(&self) -> String {
        format!("{:#?}", &self)
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err("Unsupported")),
        }
    }
}

impl From<feathr::JobStatus> for JobStatus {
    fn from(v: feathr::JobStatus) -> Self {
        match v {
            feathr::JobStatus::Starting => JobStatus::Starting,
            feathr::JobStatus::Running => JobStatus::Running,
            feathr::JobStatus::Success => JobStatus::Success,
            feathr::JobStatus::Failed => JobStatus::Failed,
        }
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct AnchorFeature(feathr::AnchorFeature);

#[pymethods]
impl AnchorFeature {
    #[getter]
    fn get_id(&self) -> String {
        feathr::Feature::get_id(&self.0).to_string()
    }
    #[getter]
    fn get_version(&self) -> u64 {
        self.0.get_version()
    }
    #[getter]
    fn get_name(&self) -> String {
        feathr::Feature::get_name(&self.0)
    }
    #[getter]
    fn get_type(&self) -> FeatureType {
        feathr::Feature::get_type(&self.0).into()
    }
    #[getter]
    fn get_key(&self) -> Vec<TypedKey> {
        feathr::Feature::get_key(&self.0)
            .into_iter()
            .map(|k| k.into())
            .collect()
    }
    #[getter]
    fn get_transformation(&self) -> Transformation {
        feathr::Feature::get_transformation(&self.0).into()
    }
    #[getter]
    fn get_key_alias(&self) -> Vec<String> {
        feathr::Feature::get_key_alias(&self.0)
    }
    #[getter]
    fn get_registry_tags(&self) -> HashMap<String, String> {
        feathr::Feature::get_registry_tags(&self.0)
    }

    fn with_key(&self, group: &str, key_alias: Vec<&str>) -> PyResult<Self> {
        block_on(async {
            Ok(self
                .0
                .with_key(group, &key_alias)
                .await
                .map_err(|e| PyValueError::new_err(format!("{}", e)))?
                .into())
        })
    }

    fn as_feature(&self, group: &str, feature_alias: &str) -> PyResult<Self> {
        block_on(async {
            Ok(self
                .0
                .as_feature(group, feature_alias)
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "AnchorFeature(id='{}', name='{}', version={})",
            self.get_id(),
            self.get_name(),
            self.get_version()
        )
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

impl From<feathr::AnchorFeature> for AnchorFeature {
    fn from(v: feathr::AnchorFeature) -> Self {
        Self(v)
    }
}

impl Into<feathr::AnchorFeature> for AnchorFeature {
    fn into(self) -> feathr::AnchorFeature {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct DerivedFeature(feathr::DerivedFeature);

#[pymethods]
impl DerivedFeature {
    #[getter]
    fn get_id(&self) -> String {
        feathr::Feature::get_id(&self.0).to_string()
    }
    #[getter]
    fn get_version(&self) -> u64 {
        self.0.get_version()
    }
    #[getter]
    fn get_name(&self) -> String {
        feathr::Feature::get_name(&self.0)
    }
    #[getter]
    fn get_type(&self) -> FeatureType {
        feathr::Feature::get_type(&self.0).into()
    }
    #[getter]
    fn get_key(&self) -> Vec<TypedKey> {
        feathr::Feature::get_key(&self.0)
            .into_iter()
            .map(|k| k.into())
            .collect()
    }
    #[getter]
    fn get_transformation(&self) -> Transformation {
        feathr::Feature::get_transformation(&self.0).into()
    }
    #[getter]
    fn get_key_alias(&self) -> Vec<String> {
        feathr::Feature::get_key_alias(&self.0)
    }
    #[getter]
    fn get_registry_tags(&self) -> HashMap<String, String> {
        feathr::Feature::get_registry_tags(&self.0)
    }

    fn with_key(&self, key_alias: Vec<&str>) -> PyResult<Self> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                Ok(self
                    .0
                    .with_key(&key_alias)
                    .await
                    .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                    .into())
            })
    }

    fn as_feature(&self, feature_alias: &str) -> PyResult<Self> {
        block_on(async {
            Ok(self
                .0
                .as_feature(feature_alias)
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }
    fn __repr__(&self) -> String {
        format!(
            "DerivedFeature(id='{}', name='{}', version={})",
            self.get_id(),
            self.get_name(),
            self.get_version()
        )
    }

    #[getter]
    fn __dict__<'p>(&self, py: Python<'p>) -> PyResult<PyObject> {
        let map: serde_json::Value = serde_json::to_value(&self.0)
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        Ok(value_to_py(map, py))
    }
}

impl From<feathr::DerivedFeature> for DerivedFeature {
    fn from(v: feathr::DerivedFeature) -> Self {
        Self(v)
    }
}

impl Into<feathr::DerivedFeature> for DerivedFeature {
    fn into(self) -> feathr::DerivedFeature {
        self.0
    }
}

#[pyclass]
#[derive(Clone, Debug)]
struct AnchorGroup(feathr::AnchorGroup);

#[pymethods]
impl AnchorGroup {
    #[getter]
    fn get_id(&self) -> String {
        feathr::AnchorGroup::get_id(&self.0).to_string()
    }
    #[getter]
    fn get_version(&self) -> u64 {
        self.0.get_version()
    }
    #[getter]
    fn get_name(&self) -> String {
        feathr::AnchorGroup::get_name(&self.0)
    }
    #[getter]
    fn get_anchor_features(&self) -> Vec<String> {
        block_on(async { self.0.get_anchor_features().await })
    }

    #[args(keys = "None", registry_tags = "None")]
    fn anchor_feature(
        &self,
        name: &str,
        feature_type: FeatureType,
        transform: &PyAny,
        keys: Option<Vec<TypedKey>>,
        registry_tags: Option<HashMap<String, String>>,
    ) -> PyResult<AnchorFeature> {
        let mut builder = self
            .0
            .anchor(name, feature_type.into())
            .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
        if let Ok(transform) = transform.extract::<String>() {
            builder.transform(transform);
        } else if let Ok(transform) = transform.extract::<Transformation>() {
            builder.transform(transform);
        } else {
            return Err(PyValueError::new_err(
                "`transform` must be string or Transformation object",
            ));
        }
        if let Some(keys) = keys {
            let keys: Vec<feathr::TypedKey> = keys.into_iter().map(|k| k.into()).collect();
            let k: Vec<&feathr::TypedKey> = keys.iter().map(|k| k).collect();
            builder.keys(&k);
        }
        if let Some(registry_tags) = registry_tags {
            for (key, value) in registry_tags.into_iter() {
                builder.add_tag(&key, &value);
            }
        }
        block_on(async {
            Ok(builder
                .build()
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }

    fn __getitem__(&self, key: &str) -> PyResult<AnchorFeature> {
        block_on(async {
            Ok(self
                .0
                .get_anchor(key)
                .await
                .map_err(|_| PyKeyError::new_err(key.to_string()))?
                .into())
        })
    }
    fn __repr__(&self) -> String {
        format!(
            "AnchorGroup(id='{}', name='{}', version={})",
            self.get_id(),
            self.get_name(),
            self.get_version()
        )
    }
}

impl From<feathr::AnchorGroup> for AnchorGroup {
    fn from(v: feathr::AnchorGroup) -> Self {
        Self(v)
    }
}

impl Into<feathr::AnchorGroup> for AnchorGroup {
    fn into(self) -> feathr::AnchorGroup {
        self.0
    }
}

#[pyclass]
struct FeathrProject(feathr::FeathrProject, FeathrClient);

#[pymethods]
impl FeathrProject {
    #[getter]
    pub fn get_id(&self) -> String {
        block_on(async { self.0.get_id().await.to_string() })
    }
    #[getter]
    pub fn get_version(&self) -> u64 {
        block_on(async { self.0.get_version().await })
    }
    #[getter]
    pub fn get_name(&self) -> String {
        block_on(async { self.0.get_name().await.to_string() })
    }
    #[getter]
    pub fn get_input_context(&self) -> Source {
        block_on(async { self.0.INPUT_CONTEXT().await.into() })
    }

    #[getter]
    pub fn get_sources(&self) -> PyResult<HashMap<String, Source>> {
        block_on(async {
            let names = self.0.get_sources().await;
            let mut ret = HashMap::new();
            for name in names {
                let source = self.0.get_source(&name).await.unwrap();
                ret.insert(name, Source(source));
            }
            Ok(ret)
        })
    }

    pub fn get_source(&self, name: &str) -> PyResult<Source> {
        block_on(async {
            Ok(self
                .0
                .get_source(name)
                .await
                .map_err(|_| PyKeyError::new_err(name.to_string()))?
                .into())
        })
    }

    #[getter]
    pub fn get_anchor_groups(&self) -> PyResult<HashMap<String, AnchorGroup>> {
        block_on(async {
            let names = self.0.get_anchor_groups().await;
            let mut ret = HashMap::new();
            for name in names {
                let group = self.0.get_anchor_group(&name).await.unwrap();
                ret.insert(name, AnchorGroup(group));
            }
            Ok(ret)
        })
    }

    #[getter]
    pub fn get_anchor_features(&self) -> PyResult<Vec<String>> {
        block_on(async { Ok(self.0.get_anchor_features().await) })
    }

    #[getter]
    pub fn get_derived_features(&self) -> PyResult<HashMap<String, DerivedFeature>> {
        block_on(async {
            let names = self.0.get_derived_features().await;
            let mut ret = HashMap::new();
            for name in names {
                let feature = self.0.get_derived_feature(&name).await.unwrap();
                ret.insert(name, DerivedFeature(feature));
            }
            Ok(ret)
        })
    }

    pub fn get_anchor_group(&self, name: &str) -> PyResult<AnchorGroup> {
        block_on(async {
            Ok(self
                .0
                .get_anchor_group(name)
                .await
                .map_err(|_| PyKeyError::new_err(name.to_string()))?
                .into())
        })
    }

    pub fn get_derived_feature(&self, name: &str) -> PyResult<DerivedFeature> {
        block_on(async {
            Ok(self
                .0
                .get_derived_feature(name)
                .await
                .map_err(|_| PyKeyError::new_err(name.to_string()))?
                .into())
        })
    }

    #[args(registry_tags = "None")]
    pub fn anchor_group(
        &self,
        name: &str,
        source: Source,
        registry_tags: Option<HashMap<String, String>>,
    ) -> PyResult<AnchorGroup> {
        let mut builder = self.0.anchor_group(name, source.into());
        if let Some(registry_tags) = registry_tags {
            for (key, value) in registry_tags.into_iter() {
                builder.add_registry_tag(&key, &value);
            }
        }
        block_on(async {
            Ok(builder
                .build()
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }

    #[args(keys = "None", registry_tags = "None")]
    pub fn derived_feature(
        &self,
        name: &str,
        feature_type: FeatureType,
        transform: &PyAny,
        inputs: &PyList,
        keys: Option<Vec<TypedKey>>,
        registry_tags: Option<HashMap<String, String>>,
    ) -> PyResult<DerivedFeature> {
        let mut builder = self.0.derived_feature(name, feature_type.into());
        if let Ok(transform) = transform.extract::<String>() {
            builder.transform(transform);
        } else if let Ok(transform) = transform.extract::<Transformation>() {
            builder.transform(transform);
        } else {
            return Err(PyValueError::new_err(
                "`transform` must be string or Transformation object",
            ));
        }
        if let Some(keys) = keys {
            let keys: Vec<feathr::TypedKey> = keys.into_iter().map(|k| k.into()).collect();
            let k: Vec<&feathr::TypedKey> = keys.iter().map(|k| k).collect();
            builder.keys(&k);
        }
        for f in inputs.iter() {
            if let Ok(f) = f.extract::<AnchorFeature>() {
                let f: feathr::AnchorFeature = f.to_owned().into();
                builder.add_input(&f);
            } else if let Ok(f) = f.extract::<DerivedFeature>() {
                let f: feathr::DerivedFeature = f.to_owned().into();
                builder.add_input(&f);
            } else {
                return Err(PyTypeError::new_err(
                    "Inputs must be list of AnchorFeature or DerivedFeature",
                ));
            }
        }
        if let Some(registry_tags) = registry_tags {
            for (key, value) in registry_tags.into_iter() {
                builder.add_tag(&key, &value);
            }
        }
        block_on(async {
            Ok(builder
                .build()
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }

    #[args(
        timestamp_column = "None",
        timestamp_column_format = "None",
        preprocessing = "None"
    )]
    pub fn hdfs_source(
        &self,
        name: &str,
        path: &str,
        timestamp_column: Option<String>,
        timestamp_column_format: Option<String>,
        preprocessing: Option<String>, // TODO: Use PyCallable?
    ) -> PyResult<Source> {
        let mut builder = self.0.hdfs_source(name, path);
        if let Some(timestamp_column) = timestamp_column {
            if let Some(timestamp_column_format) = timestamp_column_format {
                builder.time_window(&timestamp_column, &timestamp_column_format);
            } else {
                return Err(PyValueError::new_err(
                    "timestamp_column_format must not be omitted",
                ));
            }
        }

        if let Some(preprocessing) = preprocessing {
            builder.preprocessing(&preprocessing);
        }

        block_on(async {
            Ok(builder
                .build()
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }

    #[args(
        dbtable = "None",
        query = "None",
        auth = "None",
        timestamp_column = "None",
        timestamp_column_format = "None",
        preprocessing = "None"
    )]
    pub fn jdbc_source(
        &self,
        name: &str,
        url: &str,
        dbtable: Option<String>,
        query: Option<String>,
        auth: Option<JdbcSourceAuth>,
        timestamp_column: Option<String>,
        timestamp_column_format: Option<String>,
        preprocessing: Option<String>, // TODO: Use PyCallable?
    ) -> PyResult<Source> {
        let mut builder = self.0.jdbc_source(name, url);

        if let Some(dbtable) = dbtable {
            builder.dbtable(&dbtable);
        } else {
            if let Some(query) = query {
                builder.query(&query);
            } else {
                return Err(PyValueError::new_err(
                    "dbtable and query cannot be both omitted",
                ));
            }
        }

        if let Some(auth) = auth {
            builder.auth(auth.into());
        }

        if let Some(timestamp_column) = timestamp_column {
            if let Some(timestamp_column_format) = timestamp_column_format {
                builder.time_window(&timestamp_column, &timestamp_column_format);
            } else {
                return Err(PyValueError::new_err(
                    "timestamp_column_format must not be omitted",
                ));
            }
        }

        if let Some(preprocessing) = preprocessing {
            builder.preprocessing(&preprocessing);
        }

        block_on(async {
            Ok(builder
                .build()
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }

    #[args(
        mode = "None",
        timestamp_column = "None",
        timestamp_column_format = "None",
        preprocessing = "None"
    )]
    pub fn cosmosdb_source(
        &self,
        name: &str,
        endpoint: &str,
        database: &str,
        collection: &str,
        mode: Option<String>,
        timestamp_column: Option<String>,
        timestamp_column_format: Option<String>,
        preprocessing: Option<String>, // TODO: Use PyCallable?
    ) -> PyResult<Source> {
        let mut builder = self.0.generic_source(name, "cosmos.oltp");

        if let Some(mode) = mode {
            builder.mode(mode);
        }

        builder
            .option("spark.cosmos.accountEndpoint", endpoint)
            .option("spark.cosmos.database", database)
            .option("spark.cosmos.container", collection)
            .option("spark.cosmos.accountKey", format!("${{{}_KEY}}", name));

        if let Some(timestamp_column) = timestamp_column {
            if let Some(timestamp_column_format) = timestamp_column_format {
                builder.time_window(&timestamp_column, &timestamp_column_format);
            } else {
                return Err(PyValueError::new_err(
                    "timestamp_column_format must not be omitted",
                ));
            }
        }

        if let Some(preprocessing) = preprocessing {
            builder.preprocessing(&preprocessing);
        }

        block_on(async {
            Ok(builder
                .build()
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .into())
        })
    }
    // pub fn kafka_source(&self, name: &str, brokers: &PyList, topics: &PyList, avro_json: &PyAny) {}

    fn get_offline_features(
        &self,
        observation: &PyAny,
        feature_query: &PyList,
        output: &PyAny,
    ) -> PyResult<u64> {
        let observation: ObservationSettings = observation.extract()?;
        let observation = observation.0;
        let mut queries: Vec<feathr::FeatureQuery> = vec![];
        for f in feature_query.into_iter() {
            let q = if let Ok(s) = f.extract::<String>() {
                feathr::FeatureQuery::by_name(&[&s])
            } else if let Ok(f) = f.extract::<FeatureQuery>() {
                f.0
            } else {
                return Err(PyValueError::new_err(format!(
                    "feature_query must be list of strings or FeatureQuery objects"
                )));
            };
            queries.push(q);
        }
        let queries: Vec<&feathr::FeatureQuery> = queries.iter().map(|q| q).collect();

        let output: feathr::DataLocation = if let Ok(s) = output.extract::<String>() {
            s.parse()
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
        } else if let Ok(f) = output.extract::<DataLocation>() {
            f.0
        } else {
            return Err(PyValueError::new_err(format!(
                "output must be string or DataLocation object"
            )));
        };

        block_on(async {
            let request = self
                .0
                .feature_join_job(
                    observation,
                    &queries,
                    output
                        .to_argument()
                        .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
                )
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .output_location(output)
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .build();
            let client = self.1 .0.clone();
            Ok(client
                .submit_job(request)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .0)
        })
    }

    fn get_offline_features_async<'p>(
        &'p self,
        observation: &PyAny,
        feature_query: &PyList,
        output: &PyAny,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let observation: ObservationSettings = observation.extract()?;
        let observation = observation.0;
        let mut queries: Vec<feathr::FeatureQuery> = vec![];
        for f in feature_query.into_iter() {
            let q = if let Ok(s) = f.extract::<String>() {
                feathr::FeatureQuery::by_name(&[&s])
            } else if let Ok(f) = f.extract::<FeatureQuery>() {
                f.0
            } else {
                return Err(PyValueError::new_err(format!(
                    "feature_query must be list of strings or FeatureQuery objects"
                )));
            };
            queries.push(q);
        }
        let queries: Vec<feathr::FeatureQuery> = queries.iter().map(|q| q.to_owned()).collect();
        let project = self.0.clone();
        let client = self.1 .0.clone();
        let output: feathr::DataLocation = if let Ok(s) = output.extract::<String>() {
            s.parse()
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
        } else if let Ok(f) = output.extract::<DataLocation>() {
            f.0
        } else {
            return Err(PyValueError::new_err(format!(
                "output must be string or DataLocation object"
            )));
        };

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let queries: Vec<&feathr::FeatureQuery> = queries.iter().map(|q| q).collect();
            let request = project
                .feature_join_job(
                    observation,
                    &queries,
                    output
                        .to_argument()
                        .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?,
                )
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .output_location(output)
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?
                .build();
            Ok(client
                .submit_job(request)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .0)
        })
    }

    #[args(step = "DateTimeResolution::Daily")]
    fn materialize_features(
        &self,
        features: &PyList,
        start: &PyDateTime,
        end: &PyDateTime,
        step: DateTimeResolution,
        sink: &PyAny,
    ) -> PyResult<Vec<u64>> {
        let mut feature_names: Vec<String> = vec![];
        for f in features.into_iter() {
            if let Ok(f) = f.extract::<AnchorFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<DerivedFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<String>() {
                feature_names.push(f);
            }
        }

        let start: DateTime<Utc> = Utc
            .ymd(
                start.get_year(),
                start.get_month() as u32,
                start.get_day() as u32,
            )
            .and_hms(
                start.get_hour() as u32,
                start.get_minute() as u32,
                start.get_second() as u32,
            );
        let end: DateTime<Utc> = Utc
            .ymd(end.get_year(), end.get_month() as u32, end.get_day() as u32)
            .and_hms(
                end.get_hour() as u32,
                end.get_minute() as u32,
                end.get_second() as u32,
            );
        let sink: Vec<feathr::OutputSink> = if sink.is_none() {
            vec![]
        } else if let Ok(sink) = sink.extract::<RedisSink>() {
            vec![feathr::OutputSink::Redis(sink.0)]
        } else if let Ok(sink) = sink.extract::<CosmosDbSink>() {
            vec![feathr::OutputSink::Hdfs(sink.0)]
        } else if let Ok(sink) = sink.extract::<Vec<&PyAny>>() {
            let mut sinks: Vec<feathr::OutputSink> = vec![];
            for s in sink.into_iter() {
                if let Ok(sink) = s.extract::<RedisSink>() {
                    sinks.push(feathr::OutputSink::Redis(sink.0));
                } else if let Ok(sink) = s.extract::<CosmosDbSink>() {
                    sinks.push(feathr::OutputSink::Hdfs(sink.0));
                } else {
                    return Err(PyValueError::new_err(format!(
                        "sink must be RedisSink or CosmosDbSink"
                    )));
                }
            }
            sinks
        } else {
            return Err(PyTypeError::new_err(format!(
                "sink must be None or RedisSink or CosmosDbSink"
            )));
        };

        block_on(async {
            let mut builder = self
                .0
                .feature_gen_job(&feature_names, start, end, step.into())
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
            builder.sinks(&sink);

            let request = builder
                .build()
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
            let client = self.1 .0.clone();
            let jobs_ids: Vec<u64> = client
                .submit_jobs(request)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .into_iter()
                .map(|job_id| job_id.0)
                .collect();
            Ok(jobs_ids)
        })
    }

    #[args(step = "DateTimeResolution::Daily")]
    fn materialize_features_async<'p>(
        &'p self,
        features: &PyList,
        start: &PyDateTime,
        end: &PyDateTime,
        step: DateTimeResolution,
        sink: &PyAny,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let mut feature_names: Vec<String> = vec![];
        for f in features.into_iter() {
            if let Ok(f) = f.extract::<AnchorFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<DerivedFeature>() {
                feature_names.push(f.get_name());
            } else if let Ok(f) = f.extract::<String>() {
                feature_names.push(f);
            }
        }
        let start: DateTime<Utc> = Utc
            .ymd(
                start.get_year(),
                start.get_month() as u32,
                start.get_day() as u32,
            )
            .and_hms(
                start.get_hour() as u32,
                start.get_minute() as u32,
                start.get_second() as u32,
            );
        let end: DateTime<Utc> = Utc
            .ymd(end.get_year(), end.get_month() as u32, end.get_day() as u32)
            .and_hms(
                end.get_hour() as u32,
                end.get_minute() as u32,
                end.get_second() as u32,
            );
        let client = self.1 .0.clone();
        let project = self.0.clone();
        let sink: Vec<feathr::OutputSink> = if sink.is_none() {
            vec![]
        } else if let Ok(sink) = sink.extract::<RedisSink>() {
            vec![feathr::OutputSink::Redis(sink.0)]
        } else if let Ok(sink) = sink.extract::<CosmosDbSink>() {
            vec![feathr::OutputSink::Hdfs(sink.0)]
        } else if let Ok(sink) = sink.extract::<Vec<&PyAny>>() {
            let mut sinks: Vec<feathr::OutputSink> = vec![];
            for s in sink.into_iter() {
                if let Ok(sink) = s.extract::<RedisSink>() {
                    sinks.push(feathr::OutputSink::Redis(sink.0));
                } else if let Ok(sink) = s.extract::<CosmosDbSink>() {
                    sinks.push(feathr::OutputSink::Hdfs(sink.0));
                } else {
                    return Err(PyValueError::new_err(format!(
                        "sink must be RedisSink or CosmosDbSink"
                    )));
                }
            }
            sinks
        } else {
            return Err(PyTypeError::new_err(format!(
                "sink must be None or RedisSink or CosmosDbSink"
            )));
        };

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut builder = project
                .feature_gen_job(&feature_names, start, end, step.into())
                .await
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
            builder.sinks(&sink);

            let request = builder
                .build()
                .map_err(|e| PyValueError::new_err(format!("{:#?}", e)))?;
            let jobs_ids: Vec<u64> = client
                .submit_jobs(request)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .into_iter()
                .map(|job_id| job_id.0)
                .collect();
            Ok(jobs_ids)
        })
    }

    #[allow(non_snake_case)]
    #[getter]
    pub fn INPUT_CONTEXT(&self) -> Source {
        block_on(async { self.0.INPUT_CONTEXT().await.into() })
    }

    fn __repr__(&self) -> String {
        format!(
            "FeathrProject(id='{}', name='{}', version={})",
            self.get_id(),
            self.get_name(),
            self.get_version()
        )
    }
}

impl Into<feathr::FeathrProject> for FeathrProject {
    fn into(self) -> feathr::FeathrProject {
        self.0
    }
}

#[pyclass]
#[derive(Clone)]
struct FeathrClient(feathr::FeathrClient);

#[pymethods]
impl FeathrClient {
    #[new]
    fn load(config_file: String) -> PyResult<Self> {
        block_on(async {
            feathr::FeathrClient::load(config_file)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
                .map(|c| FeathrClient(c))
        })
    }

    #[staticmethod]
    fn load_async(config_file: String, py: Python<'_>) -> PyResult<&PyAny> {
        pyo3_asyncio::tokio::future_into_py(py, async move {
            feathr::FeathrClient::load(config_file)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
                .map(|c| FeathrClient(c))
        })
    }

    #[staticmethod]
    fn loads(content: &str) -> PyResult<Self> {
        let content = content.to_string();
        block_on(async move {
            feathr::FeathrClient::from_str(&content)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
                .map(|c| FeathrClient(c))
        })
    }

    #[staticmethod]
    fn loads_async<'p>(content: &'p str, py: Python<'p>) -> PyResult<&'p PyAny> {
        let content = content.to_string();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            feathr::FeathrClient::from_str(&content)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
                .map(|c| FeathrClient(c))
        })
    }

    fn load_project<'p>(&self, name: &str, py: Python<'p>) -> PyResult<FeathrProject> {
        let project = block_on(cancelable_wait(py, async move {
            self.0
                .load_project(name)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
        }))?;
        Ok(FeathrProject(project, self.clone()))
    }

    fn new_project<'p>(&self, name: &str, py: Python<'p>) -> PyResult<FeathrProject> {
        let project = block_on(cancelable_wait(py, async move {
            self.0
                .new_project(name)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))
        }))?;
        Ok(FeathrProject(project, self.clone()))
    }

    #[args(timeout = "None")]
    fn wait_for_job<'p>(
        &self,
        job_id: u64,
        timeout: Option<i64>,
        py: Python<'p>,
    ) -> PyResult<String> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        block_on(cancelable_wait(py, async {
            Ok(client
                .wait_for_job(feathr::JobId(job_id), timeout)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?)
        }))
    }

    #[args(timeout = "None")]
    fn wait_for_job_async<'p>(
        &'p self,
        id: u64,
        timeout: Option<i64>,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        pyo3_asyncio::tokio::future_into_py(py, async move {
            Ok(client
                .wait_for_job(feathr::JobId(id), timeout)
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?)
        })
    }

    #[args(timeout = "None")]
    fn wait_for_jobs<'p>(
        &self,
        job_id: Vec<u64>,
        timeout: Option<i64>,
        py: Python<'p>,
    ) -> PyResult<Vec<String>> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        block_on(cancelable_wait(py, async {
            let jobs = job_id
                .into_iter()
                .map(|job_id| client.wait_for_job(feathr::JobId(job_id), timeout));
            let complete: Vec<String> = join_all(jobs)
                .await
                .into_iter()
                .map(|r| r.unwrap_or_default())
                .collect();
            Ok(complete)
        }))
    }

    #[args(timeout = "None")]
    fn wait_for_jobs_async<'p>(
        &'p self,
        job_id: Vec<u64>,
        timeout: Option<i64>,
        py: Python<'p>,
    ) -> PyResult<&'p PyAny> {
        let client = self.0.clone();
        let timeout = timeout.map(|s| Duration::seconds(s));
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let jobs = job_id
                .into_iter()
                .map(|job_id| client.wait_for_job(feathr::JobId(job_id), timeout));
            let complete: Vec<String> = join_all(jobs)
                .await
                .into_iter()
                .map(|r| r.unwrap_or_default())
                .collect();
            Ok(complete)
        })
    }

    pub fn get_job_status(&self, job_id: u64) -> PyResult<JobStatus> {
        let client = self.0.clone();
        block_on(async {
            let status: JobStatus = client
                .get_job_status(feathr::JobId(job_id))
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .into();
            Ok(status)
        })
    }

    pub fn get_job_status_async<'p>(&'p self, job_id: u64, py: Python<'p>) -> PyResult<&'p PyAny> {
        let client = self.0.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let status: JobStatus = client
                .get_job_status(feathr::JobId(job_id))
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("{:#?}", e)))?
                .into();
            Ok(status)
        })
    }

    pub fn get_remote_url(&self, path: &str) -> String {
        self.0.get_remote_url(path)
    }
}

#[pyfunction]
fn load(config_file: String) -> PyResult<FeathrClient> {
    FeathrClient::load(config_file)
}

#[pyfunction]
fn loads(content: &str) -> PyResult<FeathrClient> {
    FeathrClient::loads(content)
}

/// A Python module implemented in Rust.
#[pymodule]
fn feathrs(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<ValueType>()?;
    m.add_class::<VectorType>()?;
    m.add_class::<TensorCategory>()?;
    m.add_class::<FeatureType>()?;
    m.add_class::<TypedKey>()?;
    m.add_class::<Aggregation>()?;
    m.add_class::<Transformation>()?;
    m.add_class::<DataLocation>()?;
    m.add_class::<Source>()?;
    m.add_class::<JdbcSourceAuth>()?;
    m.add_class::<AnchorFeature>()?;
    m.add_class::<DerivedFeature>()?;
    m.add_class::<AnchorGroup>()?;
    m.add_class::<FeatureQuery>()?;
    m.add_class::<ObservationSettings>()?;
    m.add_class::<DateTimeResolution>()?;
    m.add_class::<RedisSink>()?;
    m.add_class::<CosmosDbSink>()?;
    m.add_class::<JobStatus>()?;
    m.add_class::<FeathrProject>()?;
    m.add_class::<FeathrClient>()?;
    m.add_function(wrap_pyfunction!(load, m)?)?;
    m.add_function(wrap_pyfunction!(loads, m)?)?;
    Ok(())
}
