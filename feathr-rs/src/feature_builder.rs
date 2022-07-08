use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    feature::{
        AnchorFeature, AnchorFeatureImpl, DerivedFeature, DerivedFeatureImpl, Feature, FeatureBase,
        InputFeature,
    },
    project::{FeathrProjectImpl, FeathrProjectModifier},
    Error, FeatureType, Transformation, TypedKey,
};

#[derive(Debug)]
pub struct AnchorFeatureBuilder {
    pub(crate) owner: Arc<RwLock<FeathrProjectImpl>>,
    group: String,
    name: String,
    feature_type: FeatureType,
    transform: Option<Transformation>,
    keys: Vec<TypedKey>,
    feature_alias: String,
    registry_tags: HashMap<String, String>,
}

impl AnchorFeatureBuilder {
    pub(crate) fn new(
        owner: Arc<RwLock<FeathrProjectImpl>>,
        group: &str,
        name: &str,
        feature_type: FeatureType,
    ) -> Self {
        Self {
            owner,
            group: group.to_string(),
            name: name.to_string(),
            feature_type: feature_type,
            transform: None,
            keys: Default::default(),
            feature_alias: name.to_string(),
            registry_tags: Default::default(),
        }
    }

    pub fn transform<T>(&mut self, transform: T) -> &mut Self
    where
        T: Into<Transformation>,
    {
        self.transform = Some(transform.into());
        self
    }

    pub fn keys(&mut self, keys: &[&TypedKey]) -> &mut Self {
        self.keys = keys.into_iter().map(|&k| k.to_owned()).collect();
        self
    }

    pub fn add_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub async fn build(&mut self) -> Result<AnchorFeature, Error> {
        let anchor = AnchorFeatureImpl {
            base: FeatureBase {
                id: Uuid::new_v4(),
                name: self.name.clone(),
                feature_type: self.feature_type.to_owned(),
                key: if self.keys.is_empty() {
                    vec![TypedKey::DUMMY_KEY()]
                } else {
                    self.keys.clone()
                },
                feature_alias: self.feature_alias.clone(),
                registry_tags: self.registry_tags.clone(),
            },
            key_alias: self
                .keys
                .iter()
                .map(|k| {
                    k.key_column_alias
                        .as_ref()
                        .unwrap_or(&k.key_column)
                        .to_owned()
                })
                .collect(),
            transform: self
                .transform
                .as_ref()
                .ok_or_else(|| Error::MissingTransformation(self.name.clone()))?
                .to_owned(),
        };
        self.owner.insert_anchor(&self.group, anchor).await
    }
}
#[derive(Debug)]
pub struct DerivedFeatureBuilder {
    pub(crate) owner: Arc<RwLock<FeathrProjectImpl>>,
    name: String,
    feature_type: FeatureType,
    transform: Option<Transformation>,
    keys: Vec<TypedKey>,
    feature_alias: String,
    registry_tags: HashMap<String, String>,
    input_features: Vec<InputFeature>,
}

impl DerivedFeatureBuilder {
    pub(crate) fn new(
        owner: Arc<RwLock<FeathrProjectImpl>>,
        name: &str,
        feature_type: FeatureType,
    ) -> Self {
        Self {
            owner,
            name: name.to_string(),
            feature_type: feature_type,
            transform: None,
            keys: Default::default(),
            feature_alias: name.to_string(),
            registry_tags: Default::default(),
            input_features: Default::default(),
        }
    }

    pub fn transform<T>(&mut self, transform: T) -> &mut Self
    where
        T: Into<Transformation>,
    {
        self.transform = Some(transform.into());
        self
    }

    pub fn keys(&mut self, keys: &[&TypedKey]) -> &mut Self {
        self.keys = keys.iter().map(|&k| k.to_owned()).collect();
        self
    }

    pub fn add_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.registry_tags
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn add_input<T: Feature>(&mut self, feature: &T) -> &mut Self {
        self.input_features.push(InputFeature {
            id: feature.get_id(),
            key: feature.get_key(),
            feature: feature.get_name(),
            is_anchor_feature: feature.is_anchor_feature(),
        });
        self
    }

    pub async fn build(&mut self) -> Result<DerivedFeature, Error> {
        // Validation
        let key_alias: HashSet<String> = self
            .input_features
            .iter()
            .flat_map(|i| {
                i.key.iter().map(|k| {
                    k.key_column_alias
                        .to_owned()
                        .unwrap_or_else(|| k.key_column.to_owned())
                })
            })
            .collect();
        for k in self.keys.iter() {
            let ka = k
                .key_column_alias
                .to_owned()
                .unwrap_or_else(|| k.key_column.to_owned());
            if !key_alias.contains(&ka) {
                return Err(Error::InvalidDerivedKeyAlias(
                    self.name.to_owned(),
                    ka,
                    serde_json::to_string(&key_alias).unwrap(),
                ));
            }
        }

        let derived = DerivedFeatureImpl {
            base: FeatureBase {
                id: Uuid::new_v4(),
                name: self.name.clone(),
                feature_type: self.feature_type.to_owned(),
                key: if self.keys.is_empty() {
                    vec![TypedKey::DUMMY_KEY()]
                } else {
                    self.keys.clone()
                },
                feature_alias: self.feature_alias.clone(),
                registry_tags: self.registry_tags.clone(),
            },
            key_alias: {
                let aliases: Vec<String> = self
                    .keys
                    .iter()
                    .map(|k| {
                        k.key_column_alias
                            .as_ref()
                            .unwrap_or(&k.key_column)
                            .to_owned()
                    })
                    .collect();
                if aliases.is_empty() {
                    vec![TypedKey::DUMMY_KEY().key_column.to_owned()]
                } else {
                    aliases
                }
            },
            inputs: self
                .input_features
                .iter()
                .map(|f| (f.feature.to_owned(), f.to_owned()))
                .collect(),
            transform: self
                .transform
                .as_ref()
                .ok_or_else(|| Error::MissingTransformation(self.name.clone()))?
                .to_owned()
                .into(),
        };
        self.owner.insert_derived(derived).await
    }
}
