use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use registry_provider::{Credential, Permission, RbacRecord, Resource};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub(crate) struct RbacResource {
    pub(crate) resource: Resource,
    pub(crate) granted_by: Credential,
    pub(crate) granted_time: DateTime<Utc>,
    pub(crate) reason: String,
}

impl RbacResource {
    pub fn new(resource: Resource, granted_by: Credential, reason: String) -> Self {
        RbacResource {
            resource,
            granted_by,
            granted_time: Utc::now(),
            reason,
        }
    }
}

impl From<&Resource> for RbacResource {
    fn from(resource: &Resource) -> Self {
        Self {
            resource: resource.to_owned(),
            granted_by: Credential::RbacDisabled,
            granted_time: Utc::now(),
            reason: Default::default(),
        }
    }
}

impl PartialEq for RbacResource {
    fn eq(&self, other: &Self) -> bool {
        self.resource == other.resource
    }
}

impl PartialEq<Resource> for RbacResource {
    fn eq(&self, other: &Resource) -> bool {
        &self.resource == other
    }
}

impl Hash for RbacResource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.resource.hash(state);
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RbacMap {
    map: HashMap<Credential, HashMap<Permission, HashSet<RbacResource>>>,
}

impl RbacMap {
    pub fn check_permission(
        &self,
        credential: &Credential,
        resource: &Resource,
        permission: Permission,
    ) -> bool {
        self.map
            .get(&credential)
            .and_then(|map| map.get(&permission))
            .map(|set| set.contains(&resource.into()))
            .unwrap_or(false)
    }

    pub fn grant_permission(&mut self, grant: &RbacRecord) {
        self.map
            .entry(grant.credential.clone())
            .or_insert_with(HashMap::new)
            .entry(grant.permission)
            .or_insert_with(HashSet::new)
            .insert(RbacResource::new(
                grant.resource.clone(),
                grant.requestor.clone(),
                grant.reason.clone(),
            ));
    }

    pub fn revoke_permission(&mut self, revoke: &RbacRecord) {
        self.map
            .entry(revoke.credential.clone())
            .or_insert_with(HashMap::new)
            .entry(revoke.permission)
            .or_insert_with(HashSet::new)
            .remove(&(&revoke.resource).into());
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&Credential, &Permission, &RbacResource)> {
        self.map.iter().flat_map(|(c, pr)| {
            pr.iter()
                .flat_map(move |(p, r)| r.iter().map(move |r| (c, p, r)))
        })
    }
}
