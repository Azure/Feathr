use serde::Serialize;

use crate::{ObservationSettings, FeatureQuery};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureJoinConfig {
    #[serde(flatten)]
    pub observation_settings: ObservationSettings,
    pub feature_list: Vec<FeatureQuery>,
    pub output_path: String,
}

// TODO:
pub struct FeatureGenConfig;

