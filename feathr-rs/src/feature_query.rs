use serde::Serialize;

use crate::{TypedKey, Feature};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureQuery {
    pub feature_list: Vec<String>,
    pub key: Vec<String>,
}

impl FeatureQuery {
    pub fn new<T>(names: &[T], keys: &[&TypedKey]) -> Self
    where
        T: ToString
    {
        Self {
            feature_list: names.into_iter().map(|name| name.to_string()).collect(),
            key: keys.into_iter().map(|&keys| keys.key_column.to_owned()).collect(),
        }
    }

    pub fn by_name<T>(names: &[T]) -> Self
    where
        T: ToString
    {
        Self::new(names, &vec![&TypedKey::DUMMY_KEY()])
    }

    pub fn by_feature<T>(features: &[T]) -> Self
    where
        T: Feature
    {
        Self {
            feature_list: features.into_iter().map(|f| f.get_name()).collect(),
            key: vec![TypedKey::DUMMY_KEY().key_column],
        }
    }


    pub fn by_feature_ref<T>(features: &[&T]) -> Self
    where
        T: Feature
    {
        Self {
            feature_list: features.into_iter().map(|&f| f.get_name()).collect(),
            key: vec![TypedKey::DUMMY_KEY().key_column],
        }
    }
}

impl<T> From<&[T]> for FeatureQuery
where
    T: ToString
{
    fn from(names: &[T]) -> Self {
        FeatureQuery::by_name(names)
    }
}