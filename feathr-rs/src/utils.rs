use chrono::Duration;

use regex::Regex;

use crate::Error;

pub trait ExtDuration
where
    Self: Sized,
{
    fn minutes(minutes: u64) -> Self;
    fn hours(hours: u64) -> Self;
    fn days(days: u64) -> Self;
    fn from_str<T>(s: T) -> Result<Self, Error>
    where
        T: AsRef<str>;
}

impl ExtDuration for std::time::Duration {
    fn minutes(minutes: u64) -> Self {
        Self::from_secs(minutes * 60)
    }

    fn hours(hours: u64) -> Self {
        Self::from_secs(hours * 3600)
    }

    fn days(days: u64) -> Self {
        Self::from_secs(days * 86400)
    }

    fn from_str<T>(s: T) -> Result<Self, Error>
    where
        T: AsRef<str>,
    {
        Ok(str_to_dur(s.as_ref())?.to_std()
            .map_err(|_| Error::DurationError(s.as_ref().to_owned()))?)
    }
}

impl ExtDuration for chrono::Duration {
    fn minutes(minutes: u64) -> Self {
        Self::minutes(minutes as i64)
    }

    fn hours(hours: u64) -> Self {
        Self::hours(hours as i64)
    }

    fn days(days: u64) -> Self {
        Self::days(days as i64)
    }

    fn from_str<T>(s: T) -> Result<Self, Error>
    where
        T: AsRef<str>,
    {
        str_to_dur(s.as_ref())
    }
}

pub(crate) fn str_to_dur(s: &str) -> Result<Duration, Error> {
    let re = Regex::new(r"^([0-9]+)([a-z]*)$").unwrap();
    if let Some(caps) = re.captures(s.trim()) {
        let num: i64 = caps
            .get(1)
            .ok_or_else(|| Error::DurationError(s.to_owned()))?
            .as_str()
            .parse()
            .map_err(|_| Error::DurationError(s.to_owned()))?;
        let unit = caps
            .get(2)
            .ok_or_else(|| Error::DurationError(s.to_owned()))?
            .as_str();
        match unit {
            "ns" | "nano" | "nanos" | "nanosecond" | "nanoseconds" => Ok(Duration::nanoseconds(num)),
            "us" | "micro" | "micros" | "microsecond" | "microseconds" => {
                Ok(Duration::microseconds(num))
            }
            // Bare numbers are taken to be in milliseconds.
            // @see https://github.com/lightbend/config/blob/main/HOCON.md#duration-format
            "" | "ms" | "milli" | "millis" | "millisecond" | "milliseconds" => {
                Ok(Duration::milliseconds(num))
            }
            "s" | "second" | "seconds" => Ok(Duration::seconds(num)),
            "m" | "minute" | "minutes" => Ok(Duration::seconds(num * 60)),
            "h" | "hour" | "hours" => Ok(Duration::seconds(num * 3600)),
            "d" | "day" | "dasys" => Ok(Duration::seconds(num * 86400)),
            _ => Err(Error::DurationError(s.to_owned())),
        }
    } else {
        Err(Error::DurationError(s.to_owned()))
    }
}

pub(crate) fn dur_to_string(d: Duration) -> String {
    if (d.num_nanoseconds().unwrap() % 1000) != 0 {
        format!("{}ns", d.num_nanoseconds().unwrap())
    } else if (d.num_microseconds().unwrap() % 1000) != 0 {
        format!("{}us", d.num_microseconds().unwrap())
    } else if (d.num_milliseconds() % 1000) != 0 {
        format!("{}ms", d.num_milliseconds())
    } else if (d.num_seconds() % 60) != 0 {
        format!("{}s", d.num_seconds())
    } else if (d.num_seconds() % 3600) != 0 {
        format!("{}m", d.num_seconds() / 60)
    } else if (d.num_seconds() % 86400) != 0 {
        format!("{}h", d.num_seconds() / 3600)
    } else {
        format!("{}d", d.num_seconds() / 86400)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use crate::utils::str_to_dur;

    use super::dur_to_string;

    #[test]
    fn test_str_to_dur() {
        assert_eq!(str_to_dur("1d").unwrap(), Duration::seconds(86400));
        assert_eq!(str_to_dur("8h").unwrap(), Duration::seconds(8 * 3600));
        assert_eq!(str_to_dur("120m").unwrap(), Duration::seconds(120 * 60));
        assert_eq!(str_to_dur("54s").unwrap(), Duration::seconds(54));
        assert_eq!(str_to_dur("999ms").unwrap(), Duration::milliseconds(999));
        assert_eq!(str_to_dur("666us").unwrap(), Duration::microseconds(666));
        assert_eq!(str_to_dur("333ns").unwrap(), Duration::nanoseconds(333));
        assert_eq!(str_to_dur("777").unwrap(), Duration::milliseconds(777));
        assert!(str_to_dur("888xyz").is_err());
        assert!(str_to_dur("xyz999").is_err());
    }

    #[test]
    fn test_dur_to_str() {
        assert_eq!(dur_to_string(Duration::nanoseconds(1001)), "1001ns");
        assert_eq!(dur_to_string(Duration::nanoseconds(1000)), "1us");
        assert_eq!(dur_to_string(Duration::nanoseconds(1_000_000)), "1ms");
        assert_eq!(dur_to_string(Duration::nanoseconds(1_000_000_000)), "1s");
        assert_eq!(dur_to_string(Duration::nanoseconds(3600_000_000_000)), "1h");
        assert_eq!(dur_to_string(Duration::seconds(59)), "59s");
        assert_eq!(dur_to_string(Duration::seconds(60)), "1m");
        assert_eq!(dur_to_string(Duration::seconds(7200)), "2h");
        assert_eq!(dur_to_string(Duration::seconds(386400)), "6440m");
        assert_eq!(dur_to_string(Duration::seconds(986400)), "274h");
        assert_eq!(dur_to_string(Duration::seconds(86400)), "1d");
    }
}
