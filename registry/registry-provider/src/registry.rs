use std::{collections::HashSet, fmt::Debug};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, Edge, EdgeType, Entity, EntityPropMutator,
    EntityType, ProjectDef, RegistryError, SourceDef, ToDocString,
};

pub fn extract_version(name: &str) -> (&str, Option<u64>) {
    match name.rfind(':') {
        Some(pos) => match name[pos + 1..name.len()].parse() {
            Ok(version) => (&name[0..pos], Some(version)),
            Err(_) => {
                if &name[pos + 1..name.len()] == "latest" {
                    (&name[0..pos], None)
                } else {
                    (name, None)
                }
            }
        },
        None => (name, None),
    }
}

#[async_trait]
pub trait RegistryProvider<EntityProp>: Send + Sync
where
    // Self: Sized + Send + Sync,
    EntityProp: Clone + Debug + PartialEq + Eq + EntityPropMutator + ToDocString + Send + Sync,
{
    /**
     * Batch load entities and edges
     */
    async fn load_data(
        &mut self,
        entities: Vec<Entity<EntityProp>>,
        edges: Vec<Edge>,
    ) -> Result<(), RegistryError>;

    /**
     * Get ids of all entry points
     */
    fn get_entry_points(&self) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Get one entity by its id
     */
    fn get_entity(&self, uuid: Uuid) -> Result<Entity<EntityProp>, RegistryError>;

    /**
     * Get one entity by its qualified name
     */
    fn get_entity_by_qualified_name(
        &self,
        qualified_name: &str,
    ) -> Result<Entity<EntityProp>, RegistryError>;

    /**
     * Get multiple entities by their ids
     */
    fn get_entities(&self, uuids: HashSet<Uuid>) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Get entity id by its name
     */
    fn get_entity_id_by_qualified_name(&self, qualified_name: &str) -> Result<Uuid, RegistryError>;

    /**
     * Get all neighbors with specified connection type
     */
    fn get_neighbors(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Traversal graph from `uuid` by following edges with specific edge type
     */
    fn bfs(
        &self,
        uuid: Uuid,
        edge_type: EdgeType,
        size_limit: Option<usize>,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge>), RegistryError>;

    /**
     * Get entity ids with FTS
     */
    fn search_entity(
        &self,
        query: &str,
        types: HashSet<EntityType>,
        scope: Option<Uuid>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError>;

    /**
     * Get all entities and connections between them under a project
     */
    fn get_project(
        &self,
        qualified_name: &str,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge>), RegistryError>;

    /**
     * Create new project
     */
    async fn new_project(&mut self, definition: &ProjectDef) -> Result<Uuid, RegistryError>;

    /**
     * Create new source under specified project
     */
    async fn new_source(
        &mut self,
        project_id: Uuid,
        definition: &SourceDef,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Create new anchor under specified project
     */
    async fn new_anchor(
        &mut self,
        project_id: Uuid,
        definition: &AnchorDef,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Create new anchor feature under specified anchor
     */
    async fn new_anchor_feature(
        &mut self,
        project_id: Uuid,
        anchor_id: Uuid,
        definition: &AnchorFeatureDef,
    ) -> Result<Uuid, RegistryError>;

    /**
     * Create new derived feature under specified project
     */
    async fn new_derived_feature(
        &mut self,
        project_id: Uuid,
        definition: &DerivedFeatureDef,
    ) -> Result<Uuid, RegistryError>;

    async fn delete_entity(&mut self, id: Uuid) -> Result<(), RegistryError>;

    // Provided implementations

    /**
     * Get one entity by its qualified name
     */
    fn get_entity_by_id_or_qualified_name(
        &self,
        id_or_name: &str,
    ) -> Result<Entity<EntityProp>, RegistryError> {
        match Uuid::parse_str(id_or_name) {
            Ok(id) => self.get_entity(id),
            Err(_) => self.get_entity_by_qualified_name(id_or_name),
        }
    }

    /**
     * Get entity name
     */
    fn get_entity_name(&self, uuid: Uuid) -> Result<String, RegistryError> {
        Ok(self.get_entity(uuid)?.name)
    }

    /**
     * Get entity qualified name
     */
    fn get_entity_qualified_name(&self, uuid: Uuid) -> Result<String, RegistryError> {
        Ok(self.get_entity(uuid)?.qualified_name)
    }

    /**
     * Get entity type
     */
    fn get_entity_type(&self, uuid: Uuid) -> Result<EntityType, RegistryError> {
        Ok(self.get_entity(uuid)?.entity_type)
    }

    /**
     * Get entity id by its qualified name or id
     */
    fn get_entity_id(&self, name_or_id: &str) -> Result<Uuid, RegistryError> {
        match Uuid::parse_str(name_or_id) {
            Ok(id) => Ok(self.get_entity(id)?.id),
            Err(_) => self.get_entity_id_by_qualified_name(name_or_id),
        }
    }

    /**
     * Returns the names of all projects
     */
    fn get_project_names(&self) -> Result<Vec<String>, RegistryError> {
        Ok(self
            .get_entry_points()?
            .into_iter()
            .filter(|e| e.entity_type == EntityType::Project)
            .map(|e| e.qualified_name)
            .collect())
    }

    /**
     * Returns all entities belong to specified project
     */
    fn get_children(
        &self,
        id: Uuid,
        entity_types: HashSet<EntityType>,
    ) -> Result<Vec<Entity<EntityProp>>, RegistryError> {
        // Make sure the entity has correct type
        let et = self.get_entity(id)?.entity_type;
        if et != EntityType::Project && et != EntityType::Anchor {
            return Err(RegistryError::WrongEntityType(id, et));
        }
        // Get all ids belongs to this project
        Ok(self
            .get_neighbors(id, EdgeType::Contains)?
            .into_iter()
            .filter(|e| entity_types.contains(&e.entity_type))
            .collect())
    }

    /**
     * Returns all entities that depend on this one and vice versa, directly and indirectly
     */
    fn get_lineage(
        &self,
        id: Uuid,
        size_limit: Option<usize>,
    ) -> Result<(Vec<Entity<EntityProp>>, Vec<Edge>), RegistryError> {
        let (upstream, upstream_edges) = self.bfs(id, EdgeType::Consumes, size_limit)?;
        let (downstream, downstream_edges) = self.bfs(id, EdgeType::Produces, size_limit)?;
        Ok((
            upstream
                .into_iter()
                .chain(downstream.into_iter())
                .collect::<HashSet<Entity<EntityProp>>>()
                .into_iter()
                .collect(),
            upstream_edges
                .into_iter()
                .chain(downstream_edges.into_iter())
                .collect::<HashSet<Edge>>()
                .into_iter()
                .collect(),
        ))
    }

    fn get_all_versions(&self, qualified_name: &str) -> Vec<Entity<EntityProp>>;

    fn get_entity_version(
        &self,
        qualified_name: &str,
        version: Option<u64>,
    ) -> Result<Entity<EntityProp>, RegistryError> {
        let versions = self.get_all_versions(qualified_name);
        match version {
            Some(v) => {
                versions
                    .into_iter()
                    .find(|e| e.version == v)
                    .ok_or(RegistryError::EntityNotFound(format!(
                        "{}:{}",
                        qualified_name, v
                    )))
            }
            None => versions
                .last()
                .cloned()
                .ok_or(RegistryError::EntityNotFound(format!(
                    "{}:latest",
                    qualified_name
                ))),
        }
    }

    fn get_next_version_number(&self, qualified_name: &str) -> u64;
}

#[cfg(test)]
mod tests {
    use crate::extract_version;

    #[test]
    fn test_extract_version() {
        assert_eq!(extract_version("abc_def"), ("abc_def", None));
        assert_eq!(extract_version("abc_def:42"), ("abc_def", Some(42)));
        assert_eq!(extract_version("abc:def:42"), ("abc:def", Some(42)));
        assert_eq!(extract_version("abc_def:latest"), ("abc_def", None));
        assert_eq!(extract_version("abc_def:xyz"), ("abc_def:xyz", None));
    }
}
