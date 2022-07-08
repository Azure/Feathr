use std::{
    collections::HashMap,
    sync::Arc,
};

use serde::{ser::SerializeStruct, Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    project::{FeathrProjectImpl, FeathrProjectModifier},
    Error, FeatureType, Transformation, TypedKey, DerivedTransformation,
};

pub trait Feature
where
    Self: Sized,
{
    fn is_anchor_feature(&self) -> bool;
    fn get_id(&self) -> Uuid;
    fn get_name(&self) -> String;
    fn get_type(&self) -> FeatureType;
    fn get_key(&self) -> Vec<TypedKey>;
    fn get_transformation(&self) -> Transformation;
    fn get_key_alias(&self) -> Vec<String>;
    fn get_registry_tags(&self) -> HashMap<String, String>;
}

#[derive(Clone, Debug)]
pub struct AnchorFeature {
    pub(crate) owner: Arc<RwLock<FeathrProjectImpl>>,
    pub(crate) inner: Arc<AnchorFeatureImpl>,
}

impl AnchorFeature {
    pub async fn with_key(&self, group: &str, key_alias: &[&str]) -> Result<Self, Error> {
        self.owner
            .insert_anchor(group, self.inner.with_key(key_alias)?).await
    }

    pub async fn as_feature(&self, group: &str, feature_alias: &str) -> Result<Self, Error> {
        self.owner
            .insert_anchor(group, self.inner.as_feature(feature_alias)).await
    }
}

impl Feature for AnchorFeature {
    fn is_anchor_feature(&self) -> bool {
        true
    }

    fn get_id(&self) -> Uuid {
        self.inner.base.id
    }

    fn get_name(&self) -> String {
        self.inner.base.name.clone()
    }

    fn get_type(&self) -> FeatureType {
        self.inner.base.feature_type.clone()
    }

    fn get_key(&self) -> Vec<TypedKey> {
        self.inner.base.key.clone()
    }

    fn get_transformation(&self) -> Transformation {
        self.inner.transform.clone()
    }

    fn get_key_alias(&self) -> Vec<String> {
        self.inner.key_alias.clone()
    }

    fn get_registry_tags(&self) -> HashMap<String, String> {
        self.inner.base.registry_tags.clone()
    }
}

impl ToString for AnchorFeature {
    fn to_string(&self) -> String {
        self.get_name()
    }
}

impl ToString for &AnchorFeature {
    fn to_string(&self) -> String {
        self.get_name()
    }
}

#[derive(Clone, Debug)]
pub struct DerivedFeature {
    pub(crate) owner: Arc<RwLock<FeathrProjectImpl>>,
    pub(crate) inner: Arc<DerivedFeatureImpl>,
}

impl DerivedFeature {
    pub async fn with_key(&self, key_alias: &[&str]) -> Result<Self, Error> {
        self.owner.insert_derived(self.inner.with_key(key_alias)?).await
    }

    pub async fn as_feature(&self, feature_alias: &str) -> Result<Self, Error> {
        self.owner
            .insert_derived(self.inner.as_feature(feature_alias)).await
    }
}

impl Feature for DerivedFeature {
    fn is_anchor_feature(&self) -> bool {
        false
    }
    
    fn get_id(&self) -> Uuid {
        self.inner.base.id
    }
    
    fn get_name(&self) -> String {
        self.inner.base.name.clone()
    }

    fn get_type(&self) -> FeatureType {
        self.inner.base.feature_type.clone()
    }

    fn get_key(&self) -> Vec<TypedKey> {
        self.inner.base.key.clone()
    }

    fn get_transformation(&self) -> Transformation {
        self.inner.transform.clone().into()
    }

    fn get_key_alias(&self) -> Vec<String> {
        self.inner.key_alias.clone()
    }

    fn get_registry_tags(&self) -> HashMap<String, String> {
        self.inner.base.registry_tags.clone()
    }
}

impl ToString for DerivedFeature {
    fn to_string(&self) -> String {
        self.get_name()
    }
}

impl ToString for &DerivedFeature {
    fn to_string(&self) -> String {
        self.get_name()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FeatureBase {
    #[serde(skip)]
    pub(crate) id: Uuid,
    #[serde(skip)]
    pub(crate) name: String,
    #[serde(rename = "type", default)]
    pub(crate) feature_type: FeatureType,
    #[serde(skip)]
    pub(crate) key: Vec<TypedKey>,
    #[serde(skip)]
    pub(crate) feature_alias: String,
    #[serde(skip)]
    pub(crate) registry_tags: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct AnchorFeatureImpl {
    #[serde(flatten)]
    pub(crate) base: FeatureBase,
    #[serde(skip)]
    pub(crate) key_alias: Vec<String>,
    #[serde(flatten)]
    pub(crate) transform: Transformation,
}

impl AnchorFeatureImpl {
    fn with_key(&self, key_alias: &[&str]) -> Result<Self, Error> {
        if self.get_key().len() != key_alias.len() {
            return Err(Error::MismatchKeyAlias(
                self.get_name(),
                self.get_key().len(),
                key_alias.len(),
            ));
        }
        let mut imp = self.clone();
        imp.base.key = imp
            .base
            .key
            .iter()
            .zip(key_alias.iter())
            .map(|(key, &alias)| key.to_owned().key_column_alias(alias))
            .collect();
        Ok(imp)
    }

    fn as_feature(&self, feature_alias: &str) -> Self {
        let mut ret = self.clone();
        ret.base.feature_alias = feature_alias.to_string();
        ret
    }
}

impl Feature for AnchorFeatureImpl {
    fn is_anchor_feature(&self) -> bool {
        true
    }
    
    fn get_id(&self) -> Uuid {
        self.base.id
    }
    
    fn get_name(&self) -> String {
        self.base.name.to_owned()
    }

    fn get_type(&self) -> FeatureType {
        self.base.feature_type.to_owned()
    }

    fn get_key(&self) -> Vec<TypedKey> {
        self.base.key.to_owned()
    }

    fn get_transformation(&self) -> Transformation {
        self.transform.to_owned()
    }

    fn get_key_alias(&self) -> Vec<String> {
        self.key_alias.to_owned()
    }

    fn get_registry_tags(&self) -> HashMap<String, String> {
        self.base.registry_tags.to_owned()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub(crate) struct InputFeature {
    pub(crate) key: Vec<TypedKey>,
    pub(crate) feature: String,
    pub(crate) id: Uuid,
    pub(crate) is_anchor_feature: bool,
}

impl Serialize for InputFeature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("InputFeature", 2)?;
        state.serialize_field(
            "key",
            &self
                .key
                .iter()
                .map(|k| k.key_column_alias.clone().unwrap_or(k.key_column.clone()))
                .collect::<Vec<String>>(),
        )?;
        state.serialize_field("feature", &self.feature)?;
        state.end()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct DerivedFeatureImpl {
    #[serde(flatten)]
    pub(crate) base: FeatureBase,
    #[serde(rename = "key")]
    pub(crate) key_alias: Vec<String>,
    #[serde(flatten)]
    pub(crate) transform: DerivedTransformation,
    pub(crate) inputs: HashMap<String, InputFeature>,
}

impl DerivedFeatureImpl {
    fn with_key(&self, key_alias: &[&str]) -> Result<Self, Error> {
        let existing_key_alias: Vec<&str> = self
            .base
            .key
            .iter()
            .filter_map(|k| k.key_column_alias.as_ref())
            .map(|s| s.as_str())
            .collect();
        for &k in key_alias {
            if !existing_key_alias.contains(&k) {
                return Err(Error::KeyAliasNotFound(
                    self.get_name(),
                    k.to_string(),
                    existing_key_alias.join(", "),
                ));
            }
        }
        let mut ret = self.clone();
        ret.key_alias = key_alias.into_iter().map(|&s| s.to_owned()).collect();
        Ok(ret)
    }

    fn as_feature(&self, feature_alias: &str) -> Self {
        let mut ret = self.clone();
        ret.base.feature_alias = feature_alias.to_string();
        ret
    }
}

impl Feature for DerivedFeatureImpl {
    fn is_anchor_feature(&self) -> bool {
        false
    }
    
    fn get_id(&self) -> Uuid {
        self.base.id
    }
    
    fn get_name(&self) -> String {
        self.base.name.to_owned()
    }

    fn get_type(&self) -> FeatureType {
        self.base.feature_type.to_owned()
    }

    fn get_key(&self) -> Vec<TypedKey> {
        self.base.key.to_owned()
    }

    fn get_transformation(&self) -> Transformation {
        self.transform.to_owned().into()
    }

    fn get_key_alias(&self) -> Vec<String> {
        self.key_alias.to_owned()
    }

    fn get_registry_tags(&self) -> HashMap<String, String> {
        self.base.registry_tags.to_owned()
    }
}
