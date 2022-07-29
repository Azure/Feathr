use chrono::{DateTime, Utc};
use poem_openapi::Object;
use registry_provider::{Permission, RbacRecord};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Object)]
#[oai(rename_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub struct RbacResponse {
    pub scope: String,
    pub user_name: String,
    pub role_name: String,
    pub create_by: String,
    pub create_reason: String,
    pub create_time: DateTime<Utc>,
    pub delete_by: Option<String>,
    pub delete_reason: Option<String>,
    pub delete_time: Option<DateTime<Utc>>,
    pub access: Vec<String>,
}

pub fn into_user_roles(permissions: impl IntoIterator<Item = RbacRecord>) -> Vec<RbacResponse> {
    permissions
        .into_iter()
        .map(|record| {
            RbacResponse {
                scope: record.resource.to_string(),
                user_name: record.credential.to_string(),
                role_name: match record.permission {
                    Permission::Read => "consumer",
                    Permission::Write => "producer",
                    Permission::Admin => "admin",
                }
                .to_string(),
                create_by: record.requestor.to_string(),
                create_reason: record.reason,
                create_time: record.time,
                delete_by: None,
                delete_reason: None,
                delete_time: None,
                access: match record.permission {
                    Permission::Read => vec!["read"],
                    Permission::Write => vec!["read", "write"],
                    Permission::Admin => vec!["read", "write", "manage"],
                }
                .into_iter()
                .map(ToString::to_string)
                .collect(),
            }
        })
        .collect()
}
