mod error;
mod fts;
mod models;
mod registry;
mod rbac_provider;

pub use error::RegistryError;
pub use fts::*;
pub use models::*;
pub use registry::*;
pub use rbac_provider::*;

pub trait SerializableRegistry<'de> {
    fn take_snapshot(&self) -> Result<Vec<u8>, RegistryError>;
    fn load_snapshot(&mut self, data: &'de [u8]) -> Result<(), RegistryError>;
}