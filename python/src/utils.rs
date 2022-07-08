use chrono::Duration;
use pyo3::{exceptions::PyValueError, PyResult};
use regex::Regex;


pub(crate) fn str_to_dur(s: &str) -> PyResult<Duration> {
    let re = Regex::new(r"^([0-9]+)([a-z]*)$").unwrap();
    if let Some(caps) = re.captures(s.trim()) {
        let num: i64 = caps
            .get(1)
            .ok_or_else(|| PyValueError::new_err(s.to_owned()))?
            .as_str()
            .parse()
            .map_err(|_| PyValueError::new_err(s.to_owned()))?;
        let unit = caps
            .get(2)
            .ok_or_else(|| PyValueError::new_err(s.to_owned()))?
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
            _ => Err(PyValueError::new_err(s.to_owned())),
        }
    } else {
        Err(PyValueError::new_err(s.to_owned()))
    }
}

