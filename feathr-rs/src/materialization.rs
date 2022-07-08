use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::Error;

const END_TIME_FORMAT: &str = "yyyy-MM-dd HH:mm:ss";

mod job_date_format {
    pub fn serialize<S>(
        date: &chrono::DateTime<chrono::Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{}", date.format("%Y-%m-%d %H:%M:%S")))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DateTimeResolution {
    Daily,
    Hourly,
}

#[derive(Clone, Debug, Serialize)]
pub struct RedisSink {
    pub table_name: String,
    pub streaming: bool,
    #[serde(
        rename = "timeoutMs",
        skip_serializing_if = "Option::is_none",
        serialize_with = "ser_timeout"
    )]
    pub streaming_timeout: Option<Duration>,
}

impl RedisSink {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            streaming: false,
            streaming_timeout: None,
        }
    }

    pub fn with_timeout(table_name: &str, timeout: Duration) -> Self {
        Self {
            table_name: table_name.to_string(),
            streaming: false,
            streaming_timeout: Some(timeout),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "name", content = "params", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutputSink {
    Redis(RedisSink),
}

impl From<&OutputSink> for OutputSink {
    fn from(s: &OutputSink) -> Self {
        s.to_owned()
    }
}

impl From<RedisSink> for OutputSink {
    fn from(s: RedisSink) -> Self {
        Self::Redis(s)
    }
}

impl From<&RedisSink> for OutputSink {
    fn from(s: &RedisSink) -> Self {
        Self::Redis(s.to_owned())
    }
}

fn ser_timeout<S>(v: &Option<Duration>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match v {
        Some(dur) => ser.serialize_i64(dur.num_milliseconds()),
        None => ser.serialize_none(),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaterializationOperation {
    pub name: String,
    #[serde(with = "job_date_format")]
    pub end_time: DateTime<Utc>,
    pub end_time_format: &'static str,
    pub resolution: DateTimeResolution,
    #[serde(rename = "output")]
    pub sinks: Vec<OutputSink>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MaterializationSettings {
    pub operational: MaterializationOperation,
    #[serde(rename = "features")]
    pub feature_names: Vec<String>,
}

pub struct MaterializationSettingsBuilder {
    pub(crate) name: String,
    pub(crate) sinks: Vec<OutputSink>,
    pub(crate) features: Vec<String>,
}

impl MaterializationSettingsBuilder {
    pub fn new(name: &str, features: &[String]) -> Self {
        Self {
            name: name.to_string(),
            sinks: Default::default(),
            features: features.to_owned(),
        }
    }

    pub fn sink<T>(&mut self, sink: T) -> &mut Self
    where
        T: Into<OutputSink>,
    {
        self.sinks.push(sink.into());
        self
    }

    pub fn sinks<T>(&mut self, sinks: &[T]) -> &mut Self
    where
        T: Clone + Into<OutputSink>,
    {
        self.sinks
            .extend(sinks.into_iter().map(|s| s.to_owned().into()));
        self
    }

    pub fn build(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: DateTimeResolution,
    ) -> Result<Vec<MaterializationSettings>, Error> {
        if start >= end {
            return Err(Error::InvalidTimeRange(start, end));
        }
        let seconds = (end - start).num_seconds();
        let step_sec = match step {
            DateTimeResolution::Daily => 86400,
            DateTimeResolution::Hourly => 3600,
        };
        let ret: Vec<MaterializationSettings> = (0..seconds)
            .step_by(step_sec as usize)
            .map(|delta| {
                let end_time = end - Duration::seconds(delta);
                MaterializationSettings {
                    operational: MaterializationOperation {
                        name: self.name.clone(),
                        end_time,
                        end_time_format: END_TIME_FORMAT,
                        resolution: step,
                        sinks: self.sinks.clone(),
                    },
                    feature_names: self.features.clone(),
                }
            })
            .collect();
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use crate::*;

    #[test]
    fn ser_sink() {
        let rs = RedisSink {
            table_name: "table1".to_string(),
            streaming: true,
            streaming_timeout: Some(Duration::seconds(10)),
        };

        println!("{}", serde_json::to_string_pretty(&rs).unwrap());

        let rs = OutputSink::Redis(RedisSink {
            table_name: "table1".to_string(),
            streaming: true,
            streaming_timeout: None,
        });

        println!("{}", serde_json::to_string_pretty(&rs).unwrap());
    }

    #[test]
    fn test_build() {
        let now = Utc::now();
        let b = MaterializationSettingsBuilder::new("some_name", &[
            "abc".to_string(),
            "def".to_string(),
            "foo".to_string(),
            "bar".to_string(),
        ])
            .sink(RedisSink::new("table1"))
            .build(now - Duration::hours(3), now, DateTimeResolution::Hourly)
            .unwrap();
        println!("{}", serde_json::to_string_pretty(&b).unwrap());
        assert_eq!(b.len(), 3);
        assert_eq!(b[1].operational.name, b[0].operational.name);
    }
}
