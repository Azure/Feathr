use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::ser::SerializeStruct;
use serde::Serialize;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::client::FeathrClientImpl;
use crate::feature::{
    AnchorFeature, AnchorFeatureImpl, DerivedFeature, DerivedFeatureImpl, InputFeature,
};
use crate::feature_builder::{AnchorFeatureBuilder, DerivedFeatureBuilder};
use crate::registry_client::api_models::{EdgeType, EntityLineage, EntityType};
use crate::{
    DateTimeResolution, Error, Feature, FeatureQuery, FeatureRegistry, FeatureType,
    HdfsSourceBuilder, JdbcSourceBuilder, KafkaSourceBuilder, ObservationSettings, Source,
    SourceImpl, SourceLocation, SubmitGenerationJobRequestBuilder, SubmitJoiningJobRequestBuilder,
    TypedKey,
};

/**
 * A Feathr Project is the container of all anchor features, anchor groups, derived features, and data sources.
 */
#[derive(Clone, Debug)]
pub struct FeathrProject {
    pub(crate) inner: Arc<RwLock<FeathrProjectImpl>>,
}

impl FeathrProject {
    /**
     * Create a new Feathr project with name
     */
    pub async fn new_detached(name: &str) -> Self {
        // TODO:
        let inner = Arc::new(RwLock::new(FeathrProjectImpl {
            id: Uuid::new_v4(),
            owner: None,
            name: name.to_string(),
            anchor_groups: Default::default(),
            derivations: Default::default(),
            anchor_features: Default::default(),
            anchor_map: Default::default(),
            sources: Default::default(),
            registry_tags: Default::default(),
        }));
        inner
            .insert_source(SourceImpl::INPUT_CONTEXT())
            .await
            .unwrap(); // TODO!
        FeathrProject { inner }
    }

    /**
     * Create a new Feathr project with name
     */
    pub async fn new(owner: Arc<FeathrClientImpl>, name: &str, id: Uuid) -> Self {
        // TODO:
        let inner = Arc::new(RwLock::new(FeathrProjectImpl {
            id,
            owner: Some(owner),
            name: name.to_string(),
            anchor_groups: Default::default(),
            derivations: Default::default(),
            anchor_features: Default::default(),
            anchor_map: Default::default(),
            sources: Default::default(),
            registry_tags: Default::default(),
        }));
        inner
            .insert_source(SourceImpl::INPUT_CONTEXT())
            .await
            .unwrap(); // TODO!
        FeathrProject { inner }
    }

    pub async fn get_id(&self) -> Uuid {
        self.inner.read().await.id
    }

    pub async fn get_name(&self) -> String {
        self.inner.read().await.name.to_owned()
    }

    pub async fn get_registry_tags(&self) -> HashMap<String, String> {
        self.inner.read().await.registry_tags.to_owned()
    }

    pub async fn get_sources(&self) -> Vec<String> {
        self.inner.read().await.sources.keys().map(ToOwned::to_owned).collect()
    }

    pub async fn get_anchor_groups(&self) -> Vec<String> {
        self.inner.read().await.anchor_groups.keys().map(ToOwned::to_owned).collect()
    }

    pub async fn get_anchor_features(&self) -> Vec<String> {
        self.inner.read().await.anchor_features.keys().map(ToOwned::to_owned).collect()
    }

    pub async fn get_derived_features(&self) -> Vec<String> {
        self.inner.read().await.derivations.keys().map(ToOwned::to_owned).collect()
    }

    /**
     * Retrieve anchor feature with `name` from specified group
     */
    pub async fn get_anchor_feature(
        &self,
        group: &str,
        name: &str,
    ) -> Result<AnchorFeature, Error> {
        let r = self.inner.read().await;
        Ok(AnchorFeature {
            owner: self.inner.clone(),
            inner: r.get_anchor_feature(group, name)?,
        })
    }

    /**
     * Retrieve derived feature with `name`
     */
    pub async fn get_derived_feature(&self, name: &str) -> Result<DerivedFeature, Error> {
        let r = self.inner.read().await;
        Ok(DerivedFeature {
            owner: self.inner.clone(),
            inner: r.get_derived_feature(name)?,
        })
    }

    /**
     * Retrieve anchor group with `name`
     */
    pub async fn get_source(&self, name: &str) -> Result<Source, Error> {
        let g = self
            .inner
            .read()
            .await
            .sources
            .get(name)
            .ok_or_else(|| Error::SourceGroupNotFound(name.to_string()))?
            .clone();
        Ok(Source {
            inner: g,
        })
    }

    /**
     * Retrieve anchor group with `name`
     */
    pub async fn get_anchor_group(&self, name: &str) -> Result<AnchorGroup, Error> {
        let g = self
            .inner
            .read()
            .await
            .anchor_groups
            .get(name)
            .ok_or_else(|| Error::AnchorGroupNotFound(name.to_string()))?
            .clone();
        Ok(AnchorGroup {
            owner: self.inner.clone(),
            inner: g,
        })
    }

    /**
     * Start creating an anchor group, with given name and data source
     */
    pub fn anchor_group(&self, name: &str, source: Source) -> AnchorGroupBuilder {
        AnchorGroupBuilder::new(self.inner.clone(), name, source)
    }

    /**
     * Start creating a derived feature with given name and feature type
     */
    pub fn derived_feature(&self, name: &str, feature_type: FeatureType) -> DerivedFeatureBuilder {
        DerivedFeatureBuilder::new(self.inner.clone(), name, feature_type)
    }

    /**
     * Start creating a HDFS data source with given name
     */
    pub fn hdfs_source(&self, name: &str, path: &str) -> HdfsSourceBuilder {
        HdfsSourceBuilder::new(self.inner.clone(), name, path)
    }

    /**
     * Start creating a JDBC data source with given name
     */
    pub fn jdbc_source(&self, name: &str, url: &str) -> JdbcSourceBuilder {
        JdbcSourceBuilder::new(self.inner.clone(), name, url)
    }

    /**
     * Start creating a JDBC data source with given name
     */
    pub fn kafka_source(&self, name: &str) -> KafkaSourceBuilder {
        KafkaSourceBuilder::new(self.inner.clone(), name)
    }

    /**
     * Returns the placeholder data source
     */
    #[allow(non_snake_case)]
    pub async fn INPUT_CONTEXT(&self) -> Source {
        Source {
            inner: self.inner.read().await.sources["PASSTHROUGH"].to_owned(),
        }
    }

    /**
     * Creates the Spark job request for a feature-joining job
     */
    pub async fn feature_join_job<O, Q>(
        &self,
        observation_settings: O,
        feature_query: &[&Q],
        output: &str,
    ) -> Result<SubmitJoiningJobRequestBuilder, Error>
    where
        O: Into<ObservationSettings>,
        Q: Into<FeatureQuery> + Clone,
    {
        let fq: Vec<FeatureQuery> = feature_query.iter().map(|&q| q.clone().into()).collect();
        let feature_names: Vec<String> = fq
            .into_iter()
            .flat_map(|q| q.feature_list.into_iter())
            .collect();

        let ob = observation_settings.into();
        Ok(SubmitJoiningJobRequestBuilder::new_join(
            format!("{}_feathr_feature_join_job", self.inner.read().await.name),
            ob.observation_path.to_string(),
            self.get_feature_config().await?,
            self.get_feature_join_config(ob, feature_query, output)?,
            self.get_secret_keys().await?,
            self.get_user_functions(&feature_names).await?,
        ))
    }

    /**
     * Creates the Spark job request for a feature-generation job
     */
    pub async fn feature_gen_job<T>(
        &self,
        feature_names: &[T],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: DateTimeResolution,
    ) -> Result<SubmitGenerationJobRequestBuilder, Error>
    where
        T: ToString,
    {
        let feature_names: Vec<String> = feature_names.into_iter().map(|f| f.to_string()).collect();
        Ok(SubmitGenerationJobRequestBuilder::new_gen(
            format!(
                "{}_feathr_feature_materialization_job",
                self.inner.read().await.name
            ),
            &feature_names,
            Default::default(), // TODO:
            self.get_feature_config().await?,
            self.get_secret_keys().await?,
            start,
            end,
            step,
            self.get_user_functions(&feature_names).await?,
        ))
    }

    pub(crate) async fn get_user_functions(
        &self,
        feature_names: &[String],
    ) -> Result<HashMap<String, String>, Error> {
        Ok(self.inner.read().await.get_user_functions(feature_names))
    }

    pub(crate) async fn get_secret_keys(&self) -> Result<Vec<String>, Error> {
        Ok(self.inner.read().await.get_secret_keys())
    }

    pub(crate) async fn get_feature_config(&self) -> Result<String, Error> {
        let r = self.inner.read().await;
        let s = serde_json::to_string_pretty(&*r).unwrap();
        Ok(s)
    }

    pub(crate) fn get_feature_join_config<O, Q>(
        &self,
        observation_settings: O,
        feature_query: &[&Q],
        output: &str,
    ) -> Result<String, Error>
    where
        O: Into<ObservationSettings>,
        Q: Into<FeatureQuery> + Clone,
    {
        // TODO: Validate feature names

        #[derive(Clone, Debug, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct FeatureJoinConfig {
            #[serde(flatten)]
            observation_settings: ObservationSettings,
            feature_list: Vec<FeatureQuery>,
            output_path: String,
        }
        let cfg = FeatureJoinConfig {
            observation_settings: observation_settings.into(),
            feature_list: feature_query
                .into_iter()
                .map(|&q| q.to_owned().into())
                .collect(),
            output_path: output.to_string(),
        };
        Ok(serde_json::to_string_pretty(&cfg)?)
    }
}

#[derive(Debug)]
pub(crate) struct FeathrProjectImpl {
    pub(crate) owner: Option<Arc<FeathrClientImpl>>,
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) anchor_groups: HashMap<String, Arc<AnchorGroupImpl>>,
    pub(crate) derivations: HashMap<String, Arc<DerivedFeatureImpl>>,
    pub(crate) anchor_features: HashMap<String, Arc<AnchorFeatureImpl>>,
    pub(crate) anchor_map: HashMap<String, Vec<String>>,
    pub(crate) sources: HashMap<String, Arc<SourceImpl>>,
    pub(crate) registry_tags: HashMap<String, String>,
}

impl Serialize for FeathrProjectImpl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entity = serializer.serialize_struct("FeathrProjectImpl", 2)?;
        #[derive(Serialize)]
        struct Key {
            #[serde(rename = "sqlExpr")]
            sql_expr: Vec<String>,
        }
        #[derive(Serialize)]
        struct AnchorSer {
            key: Key,
            source: String,
            features: HashMap<String, AnchorFeatureImpl>,
        }

        let map: HashMap<_, _> = self
            .anchor_groups
            .iter()
            .map(|(name, g)| {
                let key = Key {
                    sql_expr: self.anchor_map[name]
                        .get(0)
                        .map(|fname| self.anchor_features[fname].get_key_alias())
                        .unwrap_or_default(),
                };
                let source = g.source.get_name();
                let anchors: HashMap<_, _> = self.anchor_map[name]
                    .iter()
                    .map(|f_name| {
                        (
                            f_name.to_owned(),
                            self.anchor_features[f_name].as_ref().to_owned(),
                        )
                    })
                    .collect();

                (
                    name,
                    AnchorSer {
                        key,
                        source,
                        features: anchors,
                    },
                )
            })
            .filter(|(_, a)| !a.features.is_empty())
            .collect();

        entity.serialize_field("anchors", &map)?;
        entity.serialize_field("derivations", &self.derivations)?;
        entity.serialize_field(
            "sources",
            &self
                .sources
                .iter()
                .filter(|(_, s)| !s.is_input_context())
                .collect::<HashMap<_, _>>(),
        )?;
        entity.end()
    }
}

impl FeathrProjectImpl {
    fn get_anchor_group_key_alias(&self, group: &str) -> Vec<String> {
        self.anchor_map
            .get(group)
            .map(|features| {
                features
                    .get(0)
                    .map(|fname| self.anchor_features[fname].get_key_alias())
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    fn get_anchor_feature(&self, group: &str, name: &str) -> Result<Arc<AnchorFeatureImpl>, Error> {
        self.anchor_map
            .get(group)
            .ok_or_else(|| Error::AnchorGroupNotFound(group.to_string()))?
            .iter()
            .find(|&s| s == name)
            .ok_or_else(|| Error::FeatureNotFound(name.to_string()))?;

        self.anchor_features
            .get(name)
            .cloned()
            .ok_or_else(|| Error::FeatureNotFound(name.to_string()))
    }

    fn get_derived_feature(&self, name: &str) -> Result<Arc<DerivedFeatureImpl>, Error> {
        self.derivations
            .get(name)
            .ok_or_else(|| Error::FeatureNotFound(name.to_string()))
            .map(|r| r.to_owned())
    }

    async fn insert_anchor_group(
        &mut self,
        mut group: AnchorGroupImpl,
    ) -> Result<Arc<AnchorGroupImpl>, Error> {
        if let Some(c) = self
            .owner
            .clone()
            .map(|o| o.get_registry_client())
            .flatten()
        {
            group.id = c.new_anchor(self.id, group.clone().into()).await?;
        }

        let name = group.name.clone();
        let g = Arc::new(group);
        self.anchor_groups.entry(name.clone()).or_insert(g.clone());
        self.anchor_map.entry(name).or_insert(Default::default());
        Ok(g)
    }

    async fn insert_anchor_feature(
        &mut self,
        group: &str,
        mut f: AnchorFeatureImpl,
    ) -> Result<Arc<AnchorFeatureImpl>, Error> {
        let anchors = self.anchor_map.get(group).map(Vec::len).unwrap_or_default();
        if anchors != 0 && (f.get_key_alias() != self.get_anchor_group_key_alias(group)) {
            return Err(Error::InvalidKeyAlias(f.get_name(), group.to_string()));
        }

        let g = self
            .anchor_groups
            .get_mut(group)
            .ok_or_else(|| Error::AnchorGroupNotFound(group.to_string()))?;

        if let Some(c) = self
            .owner
            .clone()
            .map(|o| o.get_registry_client())
            .flatten()
        {
            f.base.id =  c.new_anchor_feature(self.id, g.id, f.clone().into())
                .await?;
        }

        if !matches!(g.source.inner.location, SourceLocation::InputContext)
            && (f.get_key().is_empty() || f.get_key() == vec![TypedKey::DUMMY_KEY()])
        {
            return Err(Error::DummyKeyUsedWithoutInputContext(f.get_name()));
        }
        let name = f.get_name();
        self.anchor_map.get_mut(group).map(|g| g.push(name.clone()));
        let ret = Arc::new(f);
        self.anchor_features.insert(name, ret.clone());

        Ok(ret)
    }

    async fn insert_derived_feature(
        &mut self,
        mut f: DerivedFeatureImpl,
    ) -> Result<Arc<DerivedFeatureImpl>, Error> {
        if let Some(c) = self
            .owner
            .clone()
            .map(|o| o.get_registry_client())
            .flatten()
        {
            f.base.id = c.new_derived_feature(self.id, f.clone().into()).await?;
        }

        let name = f.base.name.clone();
        let ret = Arc::new(f);
        self.derivations.insert(name, ret.clone());
        Ok(ret)
    }

    async fn insert_source(&mut self, mut s: SourceImpl) -> Result<Arc<SourceImpl>, Error> {
        if let Some(c) = self
            .owner
            .clone()
            .map(|o| o.get_registry_client())
            .flatten()
        {
            s.id = c.new_source(self.id, s.clone().into()).await?;
        }

        let name = s.name.clone();
        let ret = Arc::new(s);
        self.sources.insert(name, ret.clone());
        Ok(ret)
    }

    fn get_user_functions(&self, feature_names: &[String]) -> HashMap<String, String> {
        let mut ret = HashMap::new();
        for (_, g) in &self.anchor_groups {
            if let Some(pp) = g.source.get_preprocessing() {
                let features = self.anchor_map[&g.name]
                    .iter()
                    .filter_map(|name| {
                        if feature_names.contains(name) {
                            Some(name.to_owned())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(",");
                ret.insert(features, pp);
            }
        }
        ret
    }

    fn get_secret_keys(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(|(_, s)| s.get_secret_keys().into_iter())
            .flatten()
            .collect()
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct AnchorGroupImpl {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) source: Source,
    pub(crate) registry_tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct AnchorGroup {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    inner: Arc<AnchorGroupImpl>,
}

impl AnchorGroup {
    pub fn get_id(&self) -> Uuid {
        self.inner.id
    }

    pub fn get_name(&self) -> String {
        self.inner.name.to_owned()
    }

    pub async fn get_anchor_features(&self) -> Vec<String> {
        self.owner.read().await.anchor_map[&self.inner.name].to_owned()
    }

    pub fn anchor(
        &self,
        name: &str,
        feature_type: FeatureType,
    ) -> Result<AnchorFeatureBuilder, Error> {
        Ok(AnchorFeatureBuilder::new(
            self.owner.clone(),
            &self.inner.name,
            name,
            feature_type,
        ))
    }

    pub async fn get_anchor(&self, name: &str) -> Result<AnchorFeature, Error> {
        Ok(AnchorFeature {
            owner: self.owner.clone(),
            inner: self
                .owner
                .read()
                .await
                .get_anchor_feature(&self.inner.name, name)?,
        })
    }
}

pub struct AnchorGroupBuilder {
    owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    source: Source,
    registry_tags: HashMap<String, String>,
}

impl AnchorGroupBuilder {
    fn new(owner: Arc<RwLock<FeathrProjectImpl>>, name: &str, source: Source) -> Self {
        Self {
            owner,
            name: name.to_string(),
            source: source,
            registry_tags: Default::default(),
        }
    }

    pub fn add_registry_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub async fn build(&mut self) -> Result<AnchorGroup, Error> {
        let group = AnchorGroupImpl {
            id: Uuid::new_v4(),
            name: self.name.clone(),
            source: self.source.clone(),
            registry_tags: self.registry_tags.clone(),
        };

        Ok(self.owner.insert_anchor_group(group).await?)
    }
}

#[async_trait]
pub(crate) trait FeathrProjectModifier: Sync + Send {
    async fn insert_anchor_group(&self, group: AnchorGroupImpl) -> Result<AnchorGroup, Error>;
    async fn insert_anchor(
        &self,
        group: &str,
        anchor: AnchorFeatureImpl,
    ) -> Result<AnchorFeature, Error>;
    async fn insert_derived(&self, derived: DerivedFeatureImpl) -> Result<DerivedFeature, Error>;
    async fn insert_source(&self, source: SourceImpl) -> Result<Source, Error>;
}

#[async_trait]
impl FeathrProjectModifier for Arc<RwLock<FeathrProjectImpl>> {
    async fn insert_anchor_group(&self, group: AnchorGroupImpl) -> Result<AnchorGroup, Error> {
        Ok(AnchorGroup {
            owner: self.clone(),
            inner: self.write().await.insert_anchor_group(group).await?,
        })
    }
    async fn insert_anchor(
        &self,
        group: &str,
        anchor: AnchorFeatureImpl,
    ) -> Result<AnchorFeature, Error> {
        let mut w = self.write().await;
        Ok(AnchorFeature {
            owner: self.clone(),
            inner: w.insert_anchor_feature(group, anchor).await?,
        })
    }

    async fn insert_derived(&self, derived: DerivedFeatureImpl) -> Result<DerivedFeature, Error> {
        let mut w = self.write().await;
        Ok(DerivedFeature {
            owner: self.clone(),
            inner: w.insert_derived_feature(derived).await?,
        })
    }

    async fn insert_source(&self, source: SourceImpl) -> Result<Source, Error> {
        let mut w = self.write().await;
        Ok(Source {
            inner: w.insert_source(source).await?,
        })
    }
}

impl TryFrom<EntityLineage> for FeathrProjectImpl {
    type Error = Error;

    fn try_from(value: EntityLineage) -> Result<Self, Self::Error> {
        // Build relation maps
        let belongs_map: HashMap<Uuid, String> = value
            .relations
            .iter()
            .filter(|&r| r.edge_type == EdgeType::BelongsTo)
            .map(|r| {
                (
                    r.from.to_owned(),
                    value.guid_entity_map[&r.to].name.to_owned(),
                )
            })
            .collect();
        let consumes_map: HashMap<Uuid, String> = value
            .relations
            .iter()
            .filter(|&r| r.edge_type == EdgeType::Consumes)
            .map(|r| {
                (
                    r.from.to_owned(),
                    value.guid_entity_map[&r.to].name.to_owned(),
                )
            })
            .collect();
        let (_, entity) = value
            .guid_entity_map
            .iter()
            .find(|(_, entity)| entity.get_entity_type() == EntityType::Project)
            .ok_or_else(|| Error::ProjectNotFound(Default::default()))?;
        let mut project: FeathrProjectImpl = entity.to_owned().try_into()?;
        // Add sources into project
        project.sources = value
            .guid_entity_map
            .iter()
            .filter(|(_, entity)| entity.get_entity_type() == EntityType::Source)
            .filter_map(|(_, e)| {
                e.to_owned()
                    .try_into()
                    .ok()
                    .map(|i: SourceImpl| (i.name.clone(), Arc::new(i)))
            })
            .collect();
        // Add all anchor groups into project
        project.anchor_groups = value
            .guid_entity_map
            .iter()
            .filter(|(_, entity)| entity.get_entity_type() == EntityType::Anchor)
            .filter_map(|(id, e)| {
                e.to_owned().try_into().ok().map(|mut i: AnchorGroupImpl| {
                    i.source = Source {
                        inner: project.sources[&consumes_map[id]].to_owned(),
                    };
                    (i.name.clone(), Arc::new(i))
                })
            })
            .collect();
        project.anchor_map = project
            .anchor_groups
            .iter()
            .map(|(k, _)| (k.to_owned(), Default::default()))
            .collect();
        // Find all anchor features
        let anchor_features: HashMap<Uuid, AnchorFeatureImpl> = value
            .guid_entity_map
            .iter()
            .filter(|(_, entity)| entity.get_entity_type() == EntityType::AnchorFeature)
            .filter_map(|(id, e)| e.to_owned().try_into().ok().map(|e| (id.to_owned(), e)))
            .collect();
        // Add all anchor features into corresponding anchor groups
        for (uuid, f) in anchor_features {
            let g = project.anchor_groups[&belongs_map[&uuid]].to_owned();
            if let Some(g) = project.anchor_map.get_mut(&g.name) {
                g.push(f.get_name());
            }
            project.anchor_features.insert(f.get_name(), Arc::new(f));
        }
        // Add all derived features into project
        project.derivations = value
            .guid_entity_map
            .iter()
            .filter(|(_, entity)| entity.get_entity_type() == EntityType::DerivedFeature)
            .filter_map(|(id, e)| {
                e.to_owned()
                    .try_into()
                    .ok()
                    .map(|mut i: DerivedFeatureImpl| {
                        i.inputs = value
                            .relations
                            .iter()
                            .filter(|&r| r.edge_type == EdgeType::Consumes && &r.from == id)
                            .filter_map(|r| {
                                value.guid_entity_map[&r.to].get_typed_key().ok().map(|k| {
                                    InputFeature {
                                        id: r.to,
                                        key: k,
                                        feature: value.guid_entity_map[&r.to].name.to_owned(),
                                        is_anchor_feature: value.guid_entity_map[&r.to].get_entity_type()
                                            == EntityType::AnchorFeature,
                                    }
                                })
                            })
                            .map(|f| (f.feature.clone(), f))
                            .collect();
                        (i.base.name.clone(), Arc::new(i))
                    })
            })
            .collect();

        // NOTE: returned project doesn't have owner, need to be set later
        Ok(project)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[tokio::test]
    async fn it_works() {
        let proj = FeathrProject::new_detached("p1").await;
        let s = proj
            .jdbc_source(
                "h1",
                "jdbc:sqlserver://bet-test.database.windows.net:1433;database=bet-test",
            )
            .auth(JdbcSourceAuth::Userpass)
            .dbtable("AzureRegions")
            .build()
            .await
            .unwrap();
        let g1 = proj.anchor_group("g1", s).build().await.unwrap();
        let k1 = TypedKey::new("c1", ValueType::INT32);
        let k2 = TypedKey::new("c2", ValueType::INT32);
        let f = g1
            .anchor("f1", FeatureType::INT32)
            .unwrap()
            .transform("x")
            .keys(&[&k1, &k2])
            .build()
            .await
            .unwrap();
        proj.derived_feature("d1", FeatureType::INT32)
            .add_input(&f)
            .transform("1")
            .build()
            .await
            .unwrap();
        let s = proj.get_feature_config().await.unwrap();
        println!("{}", s);
    }
}
