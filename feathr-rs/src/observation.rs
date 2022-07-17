use serde::{ser::SerializeStruct, Serialize};

use crate::{DataLocation, GetSecretKeys};

#[derive(Clone, Debug)]
pub struct ObservationSettings {
    pub observation_path: DataLocation,
    pub settings: Option<ObservationInnerSettings>,
}

impl ObservationSettings {
    pub fn new<T>(
        observation_path: T,
        timestamp_column: &str,
        format: &str,
    ) -> Result<Self, crate::Error>
    where
        T: AsRef<str>,
    {
        Ok(Self {
            observation_path: observation_path.as_ref().parse()?,
            settings: Some(ObservationInnerSettings {
                join_time_settings: JoinTimeSettings {
                    timestamp_column: TimestampColumn {
                        def: timestamp_column.to_string(),
                        format: format.into(),
                    },
                },
            }),
        })
    }

    pub fn from_path<T>(observation_path: T) -> Result<Self, crate::Error>
    where
        T: AsRef<str>,
    {
        Ok(Self {
            observation_path: observation_path.as_ref().parse()?,
            settings: None,
        })
    }
}

impl GetSecretKeys for ObservationSettings {
    fn get_secret_keys(&self) -> Vec<String> {
        self.observation_path.get_secret_keys()
    }
}

impl Serialize for ObservationSettings {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct(
            "ObservationSettings",
            if self.settings.is_none() { 1 } else { 2 },
        )?;
        match &self.observation_path {
            DataLocation::Hdfs { path } => {
                state.serialize_field("observationPath", path)?;
            }
            _ => {
                state.serialize_field("observationPath", &self.observation_path)?;
            }
        }
        if let Some(s) = &self.settings {
            state.serialize_field("settings", s)?;
        }
        state.end()
    }
}

impl<'a> Into<ObservationSettings> for &'a ObservationSettings {
    fn into(self) -> ObservationSettings {
        self.to_owned()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationInnerSettings {
    pub join_time_settings: JoinTimeSettings,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinTimeSettings {
    pub timestamp_column: TimestampColumn,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimestampColumn {
    pub def: String,
    pub format: TimestampColumnFormat,
}

#[derive(Clone, Debug)]
pub enum TimestampColumnFormat {
    Epoch,
    EpochMillis,
    Custom(String),
}

impl Serialize for TimestampColumnFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match &self {
            TimestampColumnFormat::Epoch => "epoch",
            TimestampColumnFormat::EpochMillis => "epoch_millis",
            TimestampColumnFormat::Custom(s) => s.as_str(),
        })
    }
}

impl<T> From<T> for TimestampColumnFormat
where
    T: AsRef<str>,
{
    fn from(s: T) -> Self {
        match s.as_ref().to_lowercase().as_str() {
            "epoch" => TimestampColumnFormat::Epoch,
            "epoch_millis" => TimestampColumnFormat::EpochMillis,
            _ => TimestampColumnFormat::Custom(s.as_ref().to_string()),
        }
    }
}
