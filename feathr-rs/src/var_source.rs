use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use azure_identity::DefaultAzureCredential;
use azure_security_keyvault::KeyClient;
use log::{debug, warn};

use crate::Logged;

#[async_trait]
pub trait VarSource: Sync + Send + std::fmt::Debug {
    async fn get_environment_variable(&self, name: &[&str]) -> Result<String, crate::Error>;
}

#[derive(Debug, Clone)]
struct EnvVarSource;

#[async_trait]
impl VarSource for EnvVarSource {
    async fn get_environment_variable(&self, name: &[&str]) -> Result<String, crate::Error> {
        let name: Vec<&str> = name.into_iter().map(|s| s.as_ref()).collect();
        Ok(std::env::var(name.join("__").to_uppercase())?)
    }
}

#[derive(Debug, Clone)]
struct YamlSource {
    root: serde_yaml::Value,
    overlay: EnvVarSource,
    kv_overlay: Option<KeyVaultSource>,
}

impl YamlSource {
    fn load<T>(config_path: T) -> Result<Self, crate::Error>
    where
        T: AsRef<Path>,
    {
        let f = std::fs::File::open(config_path)?;
        let root = serde_yaml::from_reader(f)?;
        Ok(Self {
            root,
            overlay: EnvVarSource,
            kv_overlay: KeyVaultSource::from_env().ok(),
        })
    }

    fn get_value_by_path<T>(
        &self,
        node: &serde_yaml::Value,
        name: &[T],
    ) -> Result<String, crate::Error>
    where
        T: AsRef<str> + Debug,
    {
        if name.is_empty() {
            return Ok(match node {
                serde_yaml::Value::String(s) => s.to_string(),
                _ => serde_yaml::to_string(node).unwrap(),
            });
        }

        let key = serde_yaml::Value::String(name[0].as_ref().to_string());

        let child = node
            .as_mapping()
            .ok_or_else(|| {
                crate::Error::InvalidConfig(format!(
                    "Current node {} is not a mapping",
                    name[0].as_ref()
                ))
            })?
            .get(&key)
            .ok_or_else(|| {
                crate::Error::InvalidConfig(format!("Key {} is missing", name[0].as_ref()))
            })?;
        self.get_value_by_path(child, &name[1..name.len()])
    }
}

impl FromStr for YamlSource {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let root = serde_yaml::from_slice(s.as_bytes())?;
        Ok(Self {
            root,
            overlay: EnvVarSource,
            kv_overlay: KeyVaultSource::from_env().ok(),
        })
    }
}

#[async_trait]
impl VarSource for YamlSource {
    async fn get_environment_variable(&self, name: &[&str]) -> Result<String, crate::Error> {
        match self.overlay.get_environment_variable(name).await {
            Ok(v) => Ok(v),
            Err(_) => match &self.kv_overlay {
                Some(kv) => match kv.get_environment_variable(name).await {
                    Ok(v) => Ok(v),
                    Err(_) => self.get_value_by_path(&self.root, name),
                },
                None => self.get_value_by_path(&self.root, name),
            },
        }
    }
}

#[derive(Debug, Clone)]
struct KeyVaultSource {
    url: String,
}

impl KeyVaultSource {
    fn new(name: &str) -> Result<Self, crate::Error> {
        if name.is_empty() {
            warn!("KeyVault is not configured.");
            return Err(crate::Error::KeyVaultNotConfigured);
        }
        Ok(Self {
            url: format!("https://{}.vault.azure.net/", name),
        })
    }

    fn from_env() -> Result<Self, crate::Error> {
        Self::new(&std::env::var("KEY_VAULT_NAME")?)
    }
}

#[async_trait]
impl VarSource for KeyVaultSource {
    async fn get_environment_variable(&self, name: &[&str]) -> Result<String, crate::Error> {
        let name = name
            .into_iter()
            .map(|s| s.as_ref())
            .collect::<Vec<&str>>()
            .join("__")
            .to_uppercase();
        // It seems that the only easy way to use KeyClient is to create on use, as it holds a reference to the credential.
        let credential = DefaultAzureCredential::default();
        let mut client = KeyClient::new(&self.url, &credential).log()?;
        Ok(client.get_secret(&name).await.log()?.value().to_owned())
    }
}

pub fn new_var_source<T>(content: T) -> Arc<dyn VarSource + Send + Sync>
where
    T: AsRef<str>,
{
    match YamlSource::from_str(content.as_ref()) {
        Ok(src) => {
            Arc::new(src)
        }
        Err(_) => {
            warn!(
                "Failed read Feathr config, using environment variables."
            );
            Arc::new(EnvVarSource)
        }
    }
}

pub fn load_var_source<T>(conf_file: T) -> Arc<dyn VarSource + Send + Sync>
where
    T: AsRef<Path>,
{
    debug!(
        "Loading Feathr config file `{}`",
        conf_file.as_ref().display()
    );
    match YamlSource::load(conf_file.as_ref()) {
        Ok(src) => {
            debug!(
                "Feathr config file `{}` loaded",
                conf_file.as_ref().display()
            );
            Arc::new(src)
        }
        Err(_) => {
            warn!(
                "Failed load Feathr config file `{}`, using environment variables.",
                conf_file.as_ref().display()
            );
            Arc::new(EnvVarSource)
        }
    }
}

pub fn default_var_source() -> Arc<dyn VarSource> {
    let conf_file: PathBuf = std::env::var("FEATHR_CONFIG")
        .ok()
        .unwrap_or_else(|| "feathr_config.yaml".to_string())
        .into();
    debug!("Loading Feathr config file `{}`", conf_file.display());

    match YamlSource::load(&conf_file) {
        Ok(src) => {
            debug!("Feathr config file `{}` loaded", conf_file.display());
            Arc::new(src)
        }
        Err(_) => {
            warn!(
                "Failed load Feathr config file `{}`, using environment variables.",
                conf_file.display()
            );
            Arc::new(EnvVarSource)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        crate::tests::init_logger();
        let y = YamlSource::load("test-script/feathr_config.yaml").unwrap();
        assert_eq!(
            y.get_environment_variable(&["project_config", "project_name"])
                .await
                .unwrap(),
            "project_feathr_integration_test"
        );
    }
}
