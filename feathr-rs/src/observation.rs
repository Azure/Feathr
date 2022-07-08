use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationSettings {
    pub observation_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<ObservationInnerSettings>,
}

impl ObservationSettings {
    pub fn new(observation_path: &str, timestamp_column: &str, format: &str) -> Self {
        Self {
            observation_path: observation_path.to_string(),
            settings: Some(ObservationInnerSettings {
                join_time_settings: JoinTimeSettings {
                    timestamp_column: TimestampColumn {
                        def: timestamp_column.to_string(),
                        format: format.into(),
                    },
                },
            }),
        }
    }

    pub fn from_path(observation_path: &str) -> Self {
        Self {
            observation_path: observation_path.to_string(),
            settings: None,
        }
    }
}

impl<T> From<T> for ObservationSettings
where
    T: AsRef<str>,
{
    fn from(s: T) -> Self {
        Self::from_path(s.as_ref())
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
