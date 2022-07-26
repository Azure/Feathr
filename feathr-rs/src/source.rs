use std::{collections::HashMap, str::FromStr, sync::Arc};

use serde::{ser::SerializeStruct, Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    project::{FeathrProjectImpl, FeathrProjectModifier},
    utils::parse_secret,
    Error, GetSecretKeys,
};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum JdbcAuth {
    Userpass { user: String, password: String },
    Token { token: String },
    Anonymous,
}

impl Serialize for JdbcAuth {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self {
            JdbcAuth::Anonymous => {
                let mut state = serializer.serialize_struct("JdbcAuth", 1)?;
                state.serialize_field("anonymous", &true)?;
                state.end()
            }
            JdbcAuth::Userpass { user, password } => {
                let mut state = serializer.serialize_struct("JdbcAuth", 2)?;
                state.serialize_field("user", user)?;
                state.serialize_field("password", password)?;
                state.end()
            }
            JdbcAuth::Token { token } => {
                let mut state = serializer.serialize_struct("JdbcAuth", 2)?;
                state.serialize_field("token", token)?;
                state.serialize_field("useToken", &true)?;
                state.end()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaSchema {
    #[serde(rename = "type")]
    type_: String,
    #[serde(rename = "avroJson")]
    avro_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
#[serde(rename_all = "camelCase")]
pub enum DataLocation {
    Hdfs {
        path: String,
    },
    Jdbc {
        url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        dbtable: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        query: Option<String>,
        #[serde(flatten)]
        auth: JdbcAuth,
    },
    Kafka {
        brokers: Vec<String>,
        topics: Vec<String>,
        schema: KafkaSchema,
    },
    Generic {
        #[serde(rename = "type", default, skip_serializing)]
        _type: String,
        format: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        mode: Option<String>,
        #[serde(flatten, default)]
        options: HashMap<String, String>,
    },
    InputContext,
}

impl Serialize for DataLocation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self {
            DataLocation::Hdfs { path } => {
                let mut state = serializer.serialize_struct("DataLocation", 1)?;
                state.serialize_field("path", path)?;
                state.end()
            }
            DataLocation::Jdbc {
                url,
                dbtable,
                query,
                auth,
            } => {
                let len = 4
                    + if matches!(auth, JdbcAuth::Anonymous) {
                        1
                    } else {
                        2
                    };
                let mut state = serializer.serialize_struct("DataLocation", len)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("url", url)?;
                match dbtable {
                    Some(dbtable) => state.serialize_field("dbtable", dbtable)?,
                    None => state.skip_field("dbtable")?,
                }
                match query {
                    Some(query) => state.serialize_field("query", query)?,
                    None => state.skip_field("query")?,
                }
                match auth {
                    JdbcAuth::Userpass { user, password } => {
                        state.serialize_field("user", user)?;
                        state.serialize_field("password", password)?;
                    }
                    JdbcAuth::Token { token } => {
                        state.serialize_field("token", token)?;
                        state.serialize_field("useToken", &true)?;
                    }
                    JdbcAuth::Anonymous => {
                        state.serialize_field("anonymous", &true)?;
                    }
                }
                state.end()
            }
            DataLocation::Kafka {
                brokers,
                topics,
                schema,
            } => {
                let mut state = serializer.serialize_struct("DataLocation", 4)?;
                state.serialize_field("type", "kafka")?;
                state.serialize_field("brokers", brokers)?;
                state.serialize_field("topics", topics)?;
                state.serialize_field("schema", schema)?;
                state.end()
            }
            DataLocation::Generic {
                _type,
                format,
                mode,
                options,
            } => {
                #[derive(Serialize)]
                struct DataLocation<'a> {
                    #[serde(rename = "type")]
                    _type: &'static str,
                    format: &'a String,
                    mode: &'a Option<String>,
                    #[serde(flatten)]
                    options: &'a HashMap<String, String>,
                }
                let wrapper = DataLocation {
                    _type: "generic",
                    format,
                    mode,
                    options,
                };
                wrapper.serialize(serializer)
            }
            DataLocation::InputContext => {
                let mut state = serializer.serialize_struct("DataLocation", 1)?;
                state.serialize_field("type", "PASSTHROUGH")?;
                state.end()
            }
        }
    }
}

impl FromStr for DataLocation {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        Ok(if s.starts_with('{') && s.ends_with('}') {
            serde_json::from_str(s)?
        } else {
            DataLocation::Hdfs {
                path: s.to_string(),
            }
        })
    }
}

impl ToString for DataLocation {
    fn to_string(&self) -> String {
        match &self {
            DataLocation::Hdfs { path } => path.to_owned(),
            _ => serde_json::to_string(&self).unwrap(),
        }
    }
}

impl DataLocation {
    pub fn to_argument(&self) -> Result<String, crate::Error> {
        match &self {
            DataLocation::Hdfs { path } => Ok(path.to_owned()),
            DataLocation::Jdbc { .. } | DataLocation::Generic { .. } => {
                Ok(serde_json::to_string(&self)?)
            }
            DataLocation::Kafka { .. } => Err(crate::Error::InvalidArgument(
                "Kafka cannot be used as output target".to_string(),
            )),
            DataLocation::InputContext => Err(crate::Error::InvalidArgument(
                "INPUT_CONTEXT cannot be used as output target".to_string(),
            )),
        }
    }

    pub fn get_type(&self) -> String {
        match &self {
            DataLocation::Hdfs { .. } => "hdfs".to_string(),
            DataLocation::Jdbc { .. } => "jdbc".to_string(),
            DataLocation::Kafka { .. } => "kafka".to_string(),
            DataLocation::Generic { .. } => "generic".to_string(),
            DataLocation::InputContext => "INPUT_CONTEXT".to_string(),
        }
    }
}

impl GetSecretKeys for DataLocation {
    fn get_secret_keys(&self) -> Vec<String> {
        let mut secrets = vec![];
        match &self {
            DataLocation::Jdbc { auth, .. } => match auth {
                JdbcAuth::Userpass { user, password } => {
                    if let Some(s) = parse_secret(&user) {
                        secrets.push(s);
                    }
                    if let Some(s) = parse_secret(&password) {
                        secrets.push(s);
                    }
                }
                JdbcAuth::Token { token } => {
                    if let Some(s) = parse_secret(&token) {
                        secrets.push(s);
                    }
                }
                JdbcAuth::Anonymous => (),
            },
            DataLocation::Generic { options, .. } => {
                for (_, v) in options {
                    if let Some(s) = parse_secret(v) {
                        secrets.push(s);
                    }
                }
            }
            _ => (),
        }
        secrets
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TimeWindowParameters {
    pub(crate) timestamp_column: String,
    pub(crate) timestamp_column_format: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SourceImpl {
    #[serde(skip)]
    pub(crate) id: Uuid,
    #[serde(skip)]
    pub(crate) version: u64,
    #[serde(skip)]
    pub(crate) name: String,
    pub(crate) location: DataLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) time_window_parameters: Option<TimeWindowParameters>,
    #[serde(skip)]
    pub(crate) preprocessing: Option<String>,
    #[serde(skip)]
    pub(crate) registry_tags: HashMap<String, String>,
}

impl Default for SourceImpl {
    fn default() -> Self {
        Self::INPUT_CONTEXT()
    }
}

impl SourceImpl {
    #[allow(non_snake_case)]
    pub(crate) fn INPUT_CONTEXT() -> SourceImpl {
        SourceImpl {
            id: Uuid::new_v4(),
            version: 1,
            name: "PASSTHROUGH".to_string(),
            location: DataLocation::InputContext,
            time_window_parameters: None,
            preprocessing: None,
            registry_tags: Default::default(),
        }
    }

    pub(crate) fn is_input_context(&self) -> bool {
        matches!(self.location, DataLocation::InputContext)
    }

    pub(crate) fn get_secret_keys(&self) -> Vec<String> {
        match &self.location {
            DataLocation::Jdbc { auth, .. } => match auth {
                JdbcAuth::Userpass { .. } => vec![
                    format!("{}_USER", self.name),
                    format!("{}_PASSWORD", self.name),
                ],
                JdbcAuth::Token { .. } => vec![format!("{}_TOKEN", self.name)],
                _ => vec![],
            },
            DataLocation::Generic { options, .. } => options
                .keys()
                .filter_map(|k| {
                    if let Some(start) = k.find("${") {
                        if let Some(end) = k[start..].find("}") {
                            Some(k[start + 2..start + end].to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect(),
            _ => vec![],
        }
    }
}

impl GetSecretKeys for SourceImpl {
    fn get_secret_keys(&self) -> Vec<String> {
        self.location.get_secret_keys()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    pub(crate) inner: Arc<SourceImpl>,
}

impl GetSecretKeys for Source {
    fn get_secret_keys(&self) -> Vec<String> {
        self.inner.get_secret_keys()
    }
}

impl Serialize for Source {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl Default for Source {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl Source {
    pub fn get_id(&self) -> Uuid {
        self.inner.id
    }

    pub fn get_version(&self) -> u64 {
        self.inner.version
    }

    pub fn get_name(&self) -> String {
        self.inner.name.clone()
    }

    pub fn get_type(&self) -> String {
        self.inner.location.get_type()
    }

    pub fn get_location(&self) -> DataLocation {
        self.inner.location.clone()
    }

    pub fn get_secret_keys(&self) -> Vec<String> {
        self.inner.get_secret_keys()
    }

    pub fn get_preprocessing(&self) -> Option<String> {
        self.inner.preprocessing.clone()
    }

    #[allow(non_snake_case)]
    pub fn INPUT_CONTEXT() -> Self {
        Self {
            inner: Arc::new(SourceImpl::INPUT_CONTEXT()),
        }
    }
}

pub struct HdfsSourceBuilder {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    path: String,
    time_window_parameters: Option<TimeWindowParameters>,
    preprocessing: Option<String>,
}

impl HdfsSourceBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrProjectImpl>>, name: &str, path: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            path: path.to_string(),
            time_window_parameters: None,
            preprocessing: None,
        }
    }

    pub fn time_window(
        &mut self,
        timestamp_column: &str,
        timestamp_column_format: &str,
    ) -> &mut Self {
        self.time_window_parameters = Some(TimeWindowParameters {
            timestamp_column: timestamp_column.to_string(),
            timestamp_column_format: timestamp_column_format.to_string(),
        });
        self
    }

    pub fn preprocessing(&mut self, preprocessing: &str) -> &mut Self {
        self.preprocessing = Some(preprocessing.to_string());
        self
    }

    pub async fn build(&self) -> Result<Source, Error> {
        let imp = SourceImpl {
            id: Uuid::new_v4(),
            version: 1,
            name: self.name.to_string(),
            location: DataLocation::Hdfs {
                path: self.path.clone(),
            },
            time_window_parameters: self.time_window_parameters.clone(),
            preprocessing: self.preprocessing.clone(),
            registry_tags: Default::default(),
        };
        self.owner.insert_source(imp).await
    }
}
pub struct JdbcSourceBuilder {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    url: String,
    dbtable: Option<String>,
    query: Option<String>,
    auth: Option<JdbcAuth>,
    time_window_parameters: Option<TimeWindowParameters>,
    preprocessing: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub enum JdbcSourceAuth {
    Anonymous,
    Userpass,
    Token,
}

impl JdbcSourceBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrProjectImpl>>, name: &str, url: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            url: url.to_string(),
            dbtable: None,
            query: None,
            auth: None,
            time_window_parameters: None,
            preprocessing: None,
        }
    }

    pub fn dbtable(&mut self, dbtable: &str) -> &mut Self {
        self.dbtable = Some(dbtable.to_string());
        self
    }

    pub fn query(&mut self, query: &str) -> &mut Self {
        self.query = Some(query.to_string());
        self
    }

    pub fn auth(&mut self, auth: JdbcSourceAuth) -> &mut Self {
        match auth {
            JdbcSourceAuth::Anonymous => self.auth = Some(JdbcAuth::Anonymous),
            JdbcSourceAuth::Userpass => {
                self.auth = Some(JdbcAuth::Userpass {
                    user: format!("${{{}_USER}}", self.name),
                    password: format!("${{{}_PASSWORD}}", self.name),
                })
            }
            JdbcSourceAuth::Token => {
                self.auth = Some(JdbcAuth::Token {
                    token: format!("${{{}_TOKEN}}", self.name),
                })
            }
        }
        self
    }

    pub fn time_window(
        &mut self,
        timestamp_column: &str,
        timestamp_column_format: &str,
    ) -> &mut Self {
        self.time_window_parameters = Some(TimeWindowParameters {
            timestamp_column: timestamp_column.to_string(),
            timestamp_column_format: timestamp_column_format.to_string(),
        });
        self
    }

    pub fn preprocessing(&mut self, preprocessing: &str) -> &mut Self {
        self.preprocessing = Some(preprocessing.to_string());
        self
    }

    pub async fn build(&self) -> Result<Source, Error> {
        let auth = self.auth.clone().unwrap_or(JdbcAuth::Anonymous);
        let imp = SourceImpl {
            id: Uuid::new_v4(),
            version: 1,
            name: self.name.to_string(),
            location: DataLocation::Jdbc {
                url: self.url.clone(),
                dbtable: self.dbtable.to_owned(),
                query: self.query.to_owned(),
                auth,
            },
            time_window_parameters: self.time_window_parameters.clone(),
            preprocessing: self.preprocessing.clone(),
            registry_tags: Default::default(),
        };
        self.owner.insert_source(imp).await
    }
}

pub struct KafkaSourceBuilder {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    brokers: Vec<String>,
    topics: Vec<String>,
    avro_json: String,
}

impl KafkaSourceBuilder {
    pub(crate) fn new(owner: Arc<RwLock<FeathrProjectImpl>>, name: &str) -> Self {
        Self {
            owner,
            name: name.to_string(),
            brokers: Default::default(),
            topics: Default::default(),
            avro_json: Default::default(),
        }
    }

    pub fn broker<T>(&mut self, broker: T) -> &mut Self
    where
        T: ToString,
    {
        self.brokers.push(broker.to_string());
        self
    }

    pub fn brokers<T>(&mut self, broker: &[T]) -> &mut Self
    where
        T: ToString,
    {
        self.brokers
            .extend(broker.into_iter().map(|s| s.to_string()));
        self
    }

    pub fn topic<T>(&mut self, topic: T) -> &mut Self
    where
        T: ToString,
    {
        self.topics.push(topic.to_string());
        self
    }

    pub fn topics<T>(&mut self, topic: &[T]) -> &mut Self
    where
        T: ToString,
    {
        self.topics.extend(topic.into_iter().map(|s| s.to_string()));
        self
    }

    pub fn avro_schema<T>(&mut self, schema: &T) -> &mut Self
    where
        T: Serialize,
    {
        self.avro_json = serde_json::to_string_pretty(schema).unwrap();
        self
    }

    pub fn avro_json<T>(&mut self, json: &T) -> &mut Self
    where
        T: ToString,
    {
        self.avro_json = json.to_string();
        self
    }

    pub async fn build(&self) -> Result<Source, Error> {
        let imp = SourceImpl {
            id: Uuid::new_v4(),
            version: 1,
            name: self.name.to_string(),
            location: DataLocation::Kafka {
                brokers: self.brokers.clone(),
                topics: self.topics.clone(),
                schema: KafkaSchema {
                    type_: "KAFKA".to_string(),
                    avro_json: self.avro_json.clone(),
                },
            },
            time_window_parameters: None,
            preprocessing: None,
            registry_tags: Default::default(),
        };
        self.owner.insert_source(imp).await
    }
}

pub struct GenericSourceBuilder {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    format: String,
    mode: Option<String>,
    options: HashMap<String, String>,
    time_window_parameters: Option<TimeWindowParameters>,
    preprocessing: Option<String>,
}

impl GenericSourceBuilder {
    pub(crate) fn new<T>(owner: Arc<RwLock<FeathrProjectImpl>>, name: &str, format: T) -> Self
    where
        T: ToString,
    {
        Self {
            owner,
            name: name.to_string(),
            format: format.to_string(),
            mode: None,
            options: Default::default(),
            time_window_parameters: None,
            preprocessing: None,
        }
    }

    pub fn mode<T>(&mut self, mode: T) -> &mut Self
    where
        T: ToString,
    {
        self.mode = Some(mode.to_string());
        self
    }

    pub fn option<T1, T2>(&mut self, key: T1, value: T2) -> &mut Self
    where
        T1: ToString,
        T2: ToString,
    {
        self.options
            .insert(key.to_string().replace('.', "__"), value.to_string());
        self
    }

    pub fn options<I, K, V>(&mut self, iter: I) -> &mut Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: ToString,
        V: ToString,
    {
        iter.into_iter().for_each(|(key, value)| {
            self.options
                .insert(key.to_string().replace('.', "__"), value.to_string());
        });
        self
    }

    pub fn time_window(
        &mut self,
        timestamp_column: &str,
        timestamp_column_format: &str,
    ) -> &mut Self {
        self.time_window_parameters = Some(TimeWindowParameters {
            timestamp_column: timestamp_column.to_string(),
            timestamp_column_format: timestamp_column_format.to_string(),
        });
        self
    }

    pub fn preprocessing(&mut self, preprocessing: &str) -> &mut Self {
        self.preprocessing = Some(preprocessing.to_string());
        self
    }

    pub async fn build(&self) -> Result<Source, Error> {
        let imp = SourceImpl {
            id: Uuid::new_v4(),
            version: 1,
            name: self.name.to_string(),
            location: DataLocation::Generic {
                _type: "generic".to_string(),
                format: self.format.clone(),
                mode: self.mode.clone(),
                options: self.options.clone(),
            },
            time_window_parameters: self.time_window_parameters.clone(),
            preprocessing: self.preprocessing.clone(),
            registry_tags: Default::default(),
        };
        self.owner.insert_source(imp).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use crate::DataLocation;

    #[test]
    fn data_location() {
        let loc = DataLocation::from_str("s3://bucket/key").unwrap();
        assert_eq!(
            loc,
            DataLocation::Hdfs {
                path: "s3://bucket/key".to_string()
            }
        );
        assert_eq!(loc.to_argument().unwrap(), "s3://bucket/key");

        let loc: DataLocation = "s3://bucket/key".parse().unwrap();
        assert_eq!(
            loc,
            DataLocation::Hdfs {
                path: "s3://bucket/key".to_string()
            }
        );
        assert_eq!(loc.to_argument().unwrap(), "s3://bucket/key");

        let loc: DataLocation = r#"{"type":"generic", "format": "cosmos.oltp", "mode": "APPEND", "spark__cosmos__accountEndpoint": "https://xchcosmos1.documents.azure.com:443/", "spark__cosmos__accountKey": "${cosmos1_KEY}", "spark__cosmos__database": "feathr", "spark__cosmos__container": "abcde"}"#.parse().unwrap();
        assert_eq!(
            loc,
            DataLocation::Generic {
                _type: "generic".to_string(),
                format: "cosmos.oltp".to_string(),
                mode: Some("APPEND".to_string()),
                options: {
                    let mut options = HashMap::new();
                    options.insert(
                        "spark__cosmos__accountEndpoint".to_string(),
                        "https://xchcosmos1.documents.azure.com:443/".to_string(),
                    );
                    options.insert(
                        "spark__cosmos__accountKey".to_string(),
                        "${cosmos1_KEY}".to_string(),
                    );
                    options.insert("spark__cosmos__database".to_string(), "feathr".to_string());
                    options.insert("spark__cosmos__container".to_string(), "abcde".to_string());
                    options
                }
            }
        );
    }
}
