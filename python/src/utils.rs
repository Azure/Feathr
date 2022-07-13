use chrono::Duration;
use futures::{Future, pin_mut};
use pyo3::{exceptions::PyValueError, PyResult, Python};
use regex::Regex;

/**
 * Check CTRL-C every second, cancel the future if pressed and return Interrupted error
 */
pub(crate) async fn cancelable_wait<'p, F, T>(py: Python<'p>, f: F) -> PyResult<T>
where
    F: Future<Output = PyResult<T>>,
{
    // Future needs to be pinned then its mutable ref can be awaited multiple times.
    pin_mut!(f);
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(1), &mut f).await {
            Ok(v) => {
                return v;
            }
            Err(_) => {
                // Timeout, check if CTRL-C is pressed
                py.check_signals()?
            }
        }
    }
}

/**
 * Run async function in blocking fashion
 */
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/**
 * Parse string into duration
 */
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

