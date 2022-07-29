mod database;
mod db_registry;
mod fts;
mod rbac_map;
mod serdes;

#[cfg(any(mock, test))]
mod mock;

use std::collections::HashSet;
use std::fmt::Debug;

use async_trait::async_trait;
pub use database::{attach_storage, load_content};
pub use db_registry::Registry;
use log::{debug, warn};
use registry_provider::{
    extract_version, AnchorDef, AnchorFeatureDef, Credential, DerivedFeatureDef, Edge, EdgeType,
    Entity, EntityPropMutator, EntityType, Permission, ProjectDef, RbacError, RbacProvider,
    RbacRecord, RegistryError, RegistryProvider, Resource, SourceDef, ToDocString,
};
use uuid::Uuid;

#[async_trait]
impl<EntityProp> RegistryProvider<EntityProp> for Registry<EntityProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + EntityPropMutator + ToDocString + Send + Sync,
{
    /**
     * Replace existing content with input snapshot
     */
    async fn load_data(
        &mut self,
        entities: Vec<Entity<EntityProp>>,
        edges: Vec<Edge>,
        permissions: Vec<RbacRecord>,
    ) -> Result<(), RegistryError> {
        self.batch_load(entities.into_iter(), edges.into_iter())
            .await?;
        self.load_permissions(permissions.into_iter())?;
        Ok(())
    }

    /**
     * Get ids of all entry points
     */
    fn get_entry_points(&self) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        Ok(self
            .entry_points
            .iter()
            .filter_map(|&idx| self.graph.node_weight(idx).cloned())
            .collect())
    }

    /**
     * Get one entity by its id
     */
    fn get_entity(&self, uuid: Uuid) -> Result<Entity<EntityProp>, RegistryError> {
        self.graph
            .node_weight(self.get_idx(uuid)?)
            .cloned()
            .ok_or(RegistryError::InvalidEntity(uuid))
    }

    /**
     * Get one entity by its qualified name
     */
    fn get_entity_by_qualified_name(
        &self,
        qualified_name: &str,
    ) -> Result<Entity<EntityProp>, RegistryError> {
        let (qualified_name, version) = extract_version(qualified_name);
        self.get_entity_by_name(qualified_name, version)
            .ok_or_else(|| RegistryError::EntityNotFound(qualified_name.to_string()))
    }

    /**
     * Get multiple entities by their ids
     */
    fn get_entities(&self, uuids: HashSet<Uuid>) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        Ok(uuids
            .into_iter()
            .filter_map(|id| {
                self.get_idx(id)
                    .ok()
                    .and_then(|idx| self.graph.node_weight(idx).cloned())
            })
            .collect())
    }

    /**
     * Get entity id by its name
     */
    fn get_entity_id_by_qualified_name(&self, qualified_name: &str) -> Result<Uuid, RegistryError> {
        let (qualified_name, version) = extract_version(qualified_name);
        self.name_id_map
            .get(qualified_name)
            .and_then(|ids| match version {
                Some(v) => ids.get(&v),
                None => ids.keys().max().and_then(|v| ids.get(v)),
            })
            .ok_or_else(|| RegistryError::EntityNotFound(qualified_name.to_string()))
            .cloned()
    }

    /**
     * Get all neighbors with specified connection type
     */
    fn get_neighbors(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        let idx = self.get_idx(uuid)?;
        Ok(self
            .get_neighbors_idx(idx, |e| e.edge_type == edge_type)
            .into_iter()
            .filter_map(|idx| self.graph.node_weight(idx).cloned())
            .collect())
    }

    /**
     * Traversal graph from `uuid` by following edges with specific edge type
     */
    fn bfs(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
        size_limit: Option<usize>,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge>), RegistryError> {
        self.bfs_traversal(uuid, size_limit, |_| true, |e| e.edge_type == edge_type)
    }

    /**
     * Get entity ids with FTS
     */
    fn search_entity(
        &self,
        query: &str,
        types: HashSet<EntityType>,
        container: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        Ok(self
            .fts_index
            .search(
                query,
                types.into_iter().map(|t| format!("{:?}", t)).collect(),
                container.map(|id| id.to_string()),
                limit,
                offset,
            )? // TODO:
            .into_iter()
            .filter_map(|id| self.get_entity_by_id(id))
            .take(limit)
            .collect())
    }

    /**
     * Get all entities and connections between them under a project
     */
    fn get_project(
        &self,
        qualified_name: &str,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge>), RegistryError> {
        let uuid = self.get_entity_id(qualified_name)?;
        let (entities, edges) = self.get_project_by_id(uuid)?;
        Ok((entities.into_iter().collect(), edges.into_iter().collect()))
    }

    // Create new project
    async fn new_project(&mut self, definition: &ProjectDef) -> Result<(Uuid, u64), RegistryError> {
        // TODO: Pre-flight validation
        let mut prop = EntityProp::new_project(definition)?;
        match self.get_all_versions(&definition.qualified_name).last() {
            // It makes no sense to create a new version of a project
            Some(e) => Ok((e.id, e.version)),
            None => {
                prop.set_version(1);
                let project_id = self
                    .insert_entity(
                        definition.id,
                        EntityType::Project,
                        &definition.qualified_name,
                        &definition.qualified_name,
                        prop,
                    )
                    .await?;
                self.index_entity(project_id, true)?;
                Ok((project_id, 1))
            }
        }
    }

    // Create new source under specified project
    async fn new_source(
        &mut self,
        project_id: Uuid,
        definition: &SourceDef,
    ) -> Result<(Uuid, u64), RegistryError> {
        // TODO: Pre-flight validation
        let mut prop = EntityProp::new_source(definition)?;

        for v in self.get_all_versions(&definition.qualified_name) {
            if v.properties == prop {
                // Found an existing version that is same as the requested one
                return Ok((v.id, v.version));
            }
        }

        let version = self.get_next_version_number(&definition.qualified_name);
        prop.set_version(version);

        let source_id = self
            .insert_entity(
                definition.id,
                EntityType::Source,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(project_id, source_id, EdgeType::Contains)
            .await?;

        self.index_entity(source_id, true)?;
        Ok((source_id, version))
    }

    // Create new anchor under specified project
    async fn new_anchor(
        &mut self,
        project_id: Uuid,
        definition: &AnchorDef,
    ) -> Result<(Uuid, u64), RegistryError> {
        if self.get_entity_by_id(definition.source_id).is_none() {
            debug!(
                "Source {} not found, cannot create anchor",
                definition.source_id
            );
            return Err(RegistryError::EntityNotFound(
                definition.source_id.to_string(),
            ));
        }

        if let Some(e) = self
            .get_all_versions(&definition.qualified_name)
            .into_iter()
            .find(|e| {
                debug!(
                    "Found existing entity {}, qualified_name '{}'",
                    e.id, e.qualified_name
                );
                // We only check source for conflicts as the anchor is always empty when it's just created
                let source = self
                    .get_neighbors(e.id, EdgeType::Consumes)
                    .expect("Data inconsistency detected");
                // An anchor has exactly one source
                assert!(source.len() == 1, "Data inconsistency detected");
                definition.source_id == source[0].id
            })
        {
            // Found existing anchor with same name and source
            return Ok((e.id, e.version));
        }

        // Create new version
        let mut prop = EntityProp::new_anchor(definition)?;
        let version = self.get_next_version_number(&definition.qualified_name);
        prop.set_version(version);

        let anchor_id = self
            .insert_entity(
                definition.id,
                EntityType::Anchor,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(project_id, anchor_id, EdgeType::Contains)
            .await?;

        self.connect(anchor_id, definition.source_id, EdgeType::Consumes)
            .await?;

        self.index_entity(anchor_id, true)?;
        Ok((anchor_id, version))
    }

    // Create new anchor feature under specified anchor
    async fn new_anchor_feature(
        &mut self,
        project_id: Uuid,
        anchor_id: Uuid,
        definition: &AnchorFeatureDef,
    ) -> Result<(Uuid, u64), RegistryError> {
        // TODO: Pre-flight validation
        let mut prop = EntityProp::new_anchor_feature(definition)?;

        if let Some(e) = self
            .get_all_versions(&definition.qualified_name)
            .into_iter()
            .find(|e| {
                debug!(
                    "Found existing entity {}, qualified_name '{}'",
                    e.id, e.qualified_name
                );

                // Found existing anchor feature same as the requested one
                prop == e.properties
            })
        {
            // Found existing anchor with same name and source
            return Ok((e.id, e.version));
        }

        let version = self.get_next_version_number(&definition.qualified_name);
        prop.set_version(version);
        let feature_id = self
            .insert_entity(
                definition.id,
                EntityType::AnchorFeature,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(project_id, feature_id, EdgeType::Contains)
            .await?;

        self.connect(anchor_id, feature_id, EdgeType::Contains)
            .await?;

        // Anchor feature also consumes source of the anchor
        let sources = self.get_neighbors(anchor_id, EdgeType::Consumes)?;
        for s in sources {
            self.connect(feature_id, s.id, EdgeType::Consumes).await?;
        }

        self.index_entity(feature_id, true)?;
        Ok((feature_id, version))
    }

    // Create new derived feature under specified project
    async fn new_derived_feature(
        &mut self,
        project_id: Uuid,
        definition: &DerivedFeatureDef,
    ) -> Result<(Uuid, u64), RegistryError> {
        let input: HashSet<Uuid> = definition
            .input_anchor_features
            .iter()
            .chain(definition.input_derived_features.iter())
            .copied()
            .collect();

        for id in input.iter() {
            if self.get_entity_by_id(*id).is_none() {
                debug!(
                    "Input feature {} not found, cannot create derived feature {}",
                    id, definition.qualified_name
                );
                return Err(RegistryError::EntityNotFound(id.to_string()));
            }
        }

        let mut prop = EntityProp::new_derived_feature(definition)?;

        if let Some(e) = self
            .get_all_versions(&definition.qualified_name)
            .into_iter()
            .find(|e| {
                debug!(
                    "Found existing entity {}, qualified_name '{}'",
                    e.id, e.qualified_name
                );
                // Check if input features in the def are same as existing one
                let upstream: HashSet<Uuid> = self
                    .get_neighbors(e.id, EdgeType::Consumes)
                    .expect("Data inconsistency detected")
                    .into_iter()
                    .map(|e| e.id)
                    .collect();
                upstream == input && prop == e.properties
            })
        {
            return Ok((e.id, e.version));
        }

        let version = self.get_next_version_number(&definition.qualified_name);
        prop.set_version(version);
        let feature_id = self
            .insert_entity(
                definition.id,
                EntityType::DerivedFeature,
                &definition.name,
                &definition.qualified_name,
                prop,
            )
            .await?;

        self.connect(project_id, feature_id, EdgeType::Contains)
            .await?;

        for &id in definition
            .input_anchor_features
            .iter()
            .chain(definition.input_derived_features.iter())
        {
            self.connect(feature_id, id, EdgeType::Consumes).await?;
        }

        self.index_entity(feature_id, true)?;
        Ok((feature_id, version))
    }

    async fn delete_entity(&mut self, id: Uuid) -> Result<(), RegistryError> {
        self.delete_entity_by_id(id).await
    }

    fn get_all_versions(&self, qualified_name: &str) -> Vec<Entity<EntityProp>> {
        let (qualified_name, _version) = extract_version(qualified_name);
        match self.name_id_map.get(qualified_name) {
            Some(ids) => ids
                .iter()
                .filter_map(|(_version, id)| self.get_entity_by_id(*id))
                .collect(),
            None => Default::default(),
        }
    }

    fn get_next_version_number(&self, qualified_name: &str) -> u64 {
        let (qualified_name, _version) = extract_version(qualified_name);
        self.name_id_map
            .get(qualified_name)
            .and_then(|ids| ids.keys().max())
            .cloned()
            .unwrap_or_default()
            + 1
    }
}

#[async_trait]
impl<EntityProp> RbacProvider for Registry<EntityProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + EntityPropMutator + ToDocString + Send + Sync,
{
    #[tracing::instrument(level = "trace", skip(self))]
    fn check_permission(
        &self,
        credential: &Credential,
        resource: &Resource,
        permission: Permission,
    ) -> Result<bool, RegistryError> {
        if credential == &Credential::RbacDisabled {
            return Ok(true);
        }
        // Get corresponding project to the resource
        let resource = match resource {
            Resource::NamedEntity(name) => {
                let id = self.get_entity_id(name)?;
                let proj_id = self.get_entity_project_id(id)?;
                Resource::Entity(proj_id)
            }
            Resource::Entity(id) => {
                let proj_id = self.get_entity_project_id(*id)?;
                Resource::Entity(proj_id)
            }
            Resource::Global => Resource::Global,
        };
        // User must be either Global Admin or Project Admin or having the permission on the resource
        Ok(self
            .permission_map
            .check_permission(credential, &Resource::Global, Permission::Admin)
            || self
                .permission_map
                .check_permission(credential, &resource, Permission::Admin)
            || self
                .permission_map
                .check_permission(credential, &resource, permission))
    }

    fn load_permissions<RI>(&mut self, permissions: RI) -> Result<(), RegistryError>
    where
        RI: Iterator<Item = RbacRecord>,
    {
        // Always use entity id as resource in the permission map
        for mut record in permissions {
            let resource = match &record.resource {
                Resource::NamedEntity(name) => match name.parse::<Uuid>() {
                    Ok(id) => Resource::Entity(id),
                    Err(_) => Resource::Entity(match self.get_entity_by_name(&name, None) {
                        Some(e) => e.id,
                        None => {
                            warn!("Entity {} not found, skipped", name);
                            continue;
                        }
                    }),
                },
                _ => record.resource,
            };
            record.resource = resource;
            self.permission_map.grant_permission(&record);
        }
        Ok(())
    }

    fn get_permissions(&self) -> Result<Vec<RbacRecord>, RegistryError> {
        self.permission_map
            .iter()
            .map(|(credential, permission, resource)| {
                Ok(RbacRecord {
                    credential: credential.to_owned(),
                    resource: self.to_named_entity_resource(&resource.resource)?,
                    permission: permission.to_owned(),
                    requestor: resource.granted_by.to_owned(),
                    reason: resource.reason.to_owned(),
                    time: resource.granted_time,
                })
            })
            .collect()
    }

    async fn grant_permission(&mut self, grant: &RbacRecord) -> Result<(), RegistryError> {
        // User `granted_by` must have the permission to grant the permission
        if !self.check_permission(&grant.requestor, &grant.resource, Permission::Admin)? {
            return Err(RbacError::PermissionDenied(
                grant.requestor.to_string(),
                grant.resource.to_owned(),
                grant.permission,
            )
            .into());
        }

        // Permission already granted, no need to do anything
        if self.check_permission(&grant.credential, &grant.resource, grant.permission)? {
            return Ok(());
        }

        let mut grant = grant.clone();

        // Resolve corresponding project id from the input resource
        grant.resource = self.to_named_entity_resource(&grant.resource)?;

        // Record permission granting info to the external storages
        for storage in self.external_storage.iter() {
            storage.write().await.grant_permission(&grant).await?;
        }

        grant.resource = self.to_entity_resource(&grant.resource)?;

        // Update local data structure
        self.permission_map.grant_permission(&grant);
        Ok(())
    }

    async fn revoke_permission(&mut self, revoke: &RbacRecord) -> Result<(), RegistryError> {
        // User `revoked_by` must have the permission to grant the permission
        if !self.check_permission(&revoke.requestor, &revoke.resource, Permission::Admin)? {
            return Err(RbacError::PermissionDenied(
                revoke.requestor.to_string(),
                revoke.resource.to_owned(),
                revoke.permission,
            )
            .into());
        }

        // Permission not granted, no need to do anything
        if !self.check_permission(&revoke.credential, &revoke.resource, revoke.permission)? {
            return Ok(());
        }

        let mut revoke = revoke.clone();

        // Always use name as resource in the external storage
        revoke.resource = self.to_named_entity_resource(&revoke.resource)?;

        revoke.resource = self.to_entity_resource(&revoke.resource)?;

        // Record permission revoking info to the external storages
        for storage in self.external_storage.iter() {
            storage.write().await.revoke_permission(&revoke).await?;
        }

        // Update local data structure
        self.permission_map.revoke_permission(&revoke);
        Ok(())
    }
}
