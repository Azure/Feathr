use std::{sync::Arc, collections::HashMap};

use serde::{ser::SerializeStruct, Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    project::{FeathrProjectImpl, FeathrProjectModifier},
    Error,
};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum JdbcAuth {
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
                let mut state = serializer.serialize_struct("JdbcAuth", 2)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("anonymous", &true)?;
                state.end()
            }
            JdbcAuth::Userpass { user, password } => {
                let mut state = serializer.serialize_struct("JdbcAuth", 3)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("user", &user)?;
                state.serialize_field("password", &password)?;
                state.end()
            }
            JdbcAuth::Token { token } => {
                let mut state = serializer.serialize_struct("JdbcAuth", 4)?;
                state.serialize_field("type", "jdbc")?;
                state.serialize_field("token", &token)?;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SourceLocation {
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
    InputContext,
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
    pub(crate) name: String,
    pub(crate) location: SourceLocation,
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
            name: "PASSTHROUGH".to_string(),
            location: SourceLocation::InputContext,
            time_window_parameters: None,
            preprocessing: None,
            registry_tags: Default::default(),
        }
    }

    pub(crate) fn is_input_context(&self) -> bool {
        matches!(self.location, SourceLocation::InputContext)
    }

    pub(crate) fn get_secret_keys(&self) -> Vec<String> {
        match &self.location {
            SourceLocation::Jdbc { auth, .. } => match auth {
                JdbcAuth::Userpass { .. } => vec![
                    format!("{}_USER", self.name),
                    format!("{}_PASSWORD", self.name),
                ],
                JdbcAuth::Token { .. } => vec![format!("{}_TOKEN", self.name)],
                _ => vec![],
            },
            _ => vec![],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Source {
    pub(crate) inner: Arc<SourceImpl>,
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

    pub fn get_name(&self) -> String {
        self.inner.name.clone()
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
            name: self.name.to_string(),
            location: SourceLocation::Hdfs {
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
        let imp = SourceImpl {
            id: Uuid::new_v4(),
            name: self.name.to_string(),
            location: SourceLocation::Jdbc {
                url: self.url.clone(),
                dbtable: self.dbtable.to_owned(),
                query: self.query.to_owned(),
                auth: self.auth.clone().unwrap_or(JdbcAuth::Anonymous),
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
            name: self.name.to_string(),
            location: SourceLocation::Kafka {
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
