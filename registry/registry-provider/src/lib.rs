mod error;
mod fts;
mod models;
mod registry;

pub use error::RegistryError;
pub use fts::*;
pub use models::*;
pub use registry::*;

pub trait SerializableRegistry<'de> {
    fn take_snapshot(&self) -> Result<Vec<u8>, RegistryError>;
    fn load_snapshot(&mut self, data: &'de [u8]) -> Result<(), RegistryError>;
}