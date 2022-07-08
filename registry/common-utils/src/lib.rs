use std::fmt::Debug;

use log::trace;

/// `set!` macro works like `vec!`, but generates a HashSet.
#[macro_export]
macro_rules! set {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_set = HashSet::new();
            $(temp_set.insert($x);)*
            temp_set
        }
    };
}

/// Log if `Result` is an error
pub trait Logged {
    fn log(self) -> Self;
}

impl<T, E> Logged for Result<T, E>
where
    E: Debug,
{
    fn log(self) -> Self {
        if let Err(e) = &self {
            trace!("---TraceError--- {:#?}", e)
        }
        self
    }
}

pub trait Appliable
where
    Self: Sized,
{
    /**
     * Use `apply` if you need to access or mutate `self`
     */
    fn apply<F>(self, f: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        f(self)
    }

    /**
     * Use `then` if you need to do something irrelevant
     */
    fn then<F>(self, f: F) -> Self
    where
        F: FnOnce(),
    {
        f();
        self
    }
}

/**
 * `Appliable` be default is implemented for all sized types
 */
impl<T> Appliable for T where T: Sized {}

/**
 * Flip `Option<Result<T, E>>` to `Result<Option<T>, E>` so we can use `?` on the result
 */
pub trait FlippedOptionResult<T, E>
{
    fn flip(self) -> Result<Option<T>, E>;
}

impl<T, E> FlippedOptionResult<T, E> for Option<Result<T, E>> {
    fn flip(self) -> Result<Option<T>, E> {
        self.map_or(Ok(None), |v| v.map(Some))
    }
}

pub fn is_default<T>(t: &T) -> bool
where
    T: Default + Eq
{
    t==&T::default()
}

pub trait Blank {
    fn is_blank(&self) -> bool;
}

impl Blank for Option<String> {
    fn is_blank(&self) -> bool {
        match &self {
            Some(s) => s.is_empty(),
            None => true,
        }
    }
}

static LOGGER: std::sync::Once = std::sync::Once::new();

pub fn init_logger() {
    LOGGER.call_once(|| {
        dotenv::dotenv().ok();
        let modules = [
            "atlas_provider",
            "common_utils",
            "feathr_registry",
            "registry_cli",
            "registry_provider",
            "sql_provider",
            "raft_registry",
            "registry_api",
            "registry_app",
        ];
        let module_logs = modules.into_iter().map(|m| format!("{}=debug", m)).collect::<Vec<_>>().join(",");
        let rust_log = format!("info,tantivy=warn,tiberius=warn,openraft=warn,{}", module_logs);
        if std::env::var_os("RUST_LOG").is_none() {
            std::env::set_var("RUST_LOG", &rust_log);
        }
        tracing_subscriber::fmt::init();
    });
}
