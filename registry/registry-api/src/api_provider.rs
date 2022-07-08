use std::collections::HashSet;

use async_trait::async_trait;
use common_utils::{set, Blank};
use log::debug;
use registry_provider::{Edge, EdgeType, EntityProperty, RegistryError, RegistryProvider};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureDef, ApiError, DerivedFeatureDef, Entities, Entity, EntityAttributes,
    EntityLineage, EntityRef, IntoApiResult, ProjectDef, SourceDef,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FeathrApiRequest {
    GetProjects {
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProject {
        id_or_name: String,
    },
    GetProjectLineage {
        id_or_name: String,
    },
    GetProjectFeatures {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    CreateProject {
        definition: ProjectDef,
    },
    GetProjectDataSources {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProjectDataSource {
        project_id_or_name: String,
        id_or_name: String,
    },
    GetProjectDataSourceVersions {
        project_id_or_name: String,
        id_or_name: String,
    },
    GetProjectDataSourceVersion {
        project_id_or_name: String,
        id_or_name: String,
        version: Option<u64>,
    },
    CreateProjectDataSource {
        project_id_or_name: String,
        definition: SourceDef,
    },
    GetProjectAnchors {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProjectAnchor {
        project_id_or_name: String,
        id_or_name: String,
    },
    GetProjectAnchorVersions {
        project_id_or_name: String,
        id_or_name: String,
    },
    GetProjectAnchorVersion {
        project_id_or_name: String,
        id_or_name: String,
        version: Option<u64>,
    },
    CreateProjectAnchor {
        project_id_or_name: String,
        definition: AnchorDef,
    },
    GetProjectDerivedFeatures {
        project_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetProjectDerivedFeature {
        project_id_or_name: String,
        id_or_name: String,
    },
    GetProjectDerivedFeatureVersions {
        project_id_or_name: String,
        id_or_name: String,
    },
    GetProjectDerivedFeatureVersion {
        project_id_or_name: String,
        id_or_name: String,
        version: Option<u64>,
    },
    CreateProjectDerivedFeature {
        project_id_or_name: String,
        definition: DerivedFeatureDef,
    },
    GetAnchorFeatures {
        project_id_or_name: String,
        anchor_id_or_name: String,
        keyword: Option<String>,
        size: Option<usize>,
        offset: Option<usize>,
    },
    GetAnchorFeature {
        project_id_or_name: String,
        anchor_id_or_name: String,
        id_or_name: String,
    },
    GetAnchorFeatureVersions {
        project_id_or_name: String,
        anchor_id_or_name: String,
        id_or_name: String,
    },
    GetAnchorFeatureVersion {
        project_id_or_name: String,
        anchor_id_or_name: String,
        id_or_name: String,
        version: Option<u64>,
    },
    CreateAnchorFeature {
        project_id_or_name: String,
        anchor_id_or_name: String,
        definition: AnchorFeatureDef,
    },
    GetFeature {
        id_or_name: String,
    },
    GetFeatureLineage {
        id_or_name: String,
    },
    BatchLoad {
        entities: Vec<registry_provider::Entity<EntityProperty>>,
        edges: Vec<Edge>,
    },
}

impl FeathrApiRequest {
    pub fn is_writing_request(&self) -> bool {
        matches!(
            &self,
            Self::CreateProject { .. }
                | Self::CreateProjectDataSource { .. }
                | Self::CreateProjectAnchor { .. }
                | Self::CreateAnchorFeature { .. }
                | Self::CreateProjectDerivedFeature { .. }
                | Self::BatchLoad { .. }
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FeathrApiResponse {
    Error(ApiError),

    Unit,
    Uuid(Uuid),
    EntityNames(Vec<String>),
    Entity(Entity),
    Entities(Entities),
    EntityLineage(EntityLineage),
}

impl FeathrApiResponse {
    pub fn into_uuid(self) -> poem::Result<Uuid> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::Uuid(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }

    pub fn into_entity_names(self) -> poem::Result<Vec<String>> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::EntityNames(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }

    pub fn into_entity(self) -> poem::Result<Entity> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::Entity(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }

    pub fn into_entities(self) -> poem::Result<Entities> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::Entities(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }
    pub fn into_lineage(self) -> poem::Result<EntityLineage> {
        match self {
            FeathrApiResponse::Error(e) => Err(e.into()),
            FeathrApiResponse::EntityLineage(v) => Ok(v),
            _ => panic!("Shouldn't reach here"),
        }
    }
}

impl From<RegistryError> for FeathrApiResponse {
    fn from(v: RegistryError) -> Self {
        Self::Error(v.into())
    }
}

impl From<()> for FeathrApiResponse {
    fn from(_: ()) -> Self {
        Self::Unit
    }
}

impl From<Uuid> for FeathrApiResponse {
    fn from(v: Uuid) -> Self {
        Self::Uuid(v)
    }
}

impl From<Vec<String>> for FeathrApiResponse {
    fn from(v: Vec<String>) -> Self {
        Self::EntityNames(v)
    }
}

impl From<Entity> for FeathrApiResponse {
    fn from(v: Entity) -> Self {
        Self::Entity(v)
    }
}

impl From<Vec<Entity>> for FeathrApiResponse {
    fn from(v: Vec<Entity>) -> Self {
        Self::Entities(Entities { entities: v })
    }
}

impl From<registry_provider::Entity<EntityProperty>> for FeathrApiResponse {
    fn from(v: registry_provider::Entity<EntityProperty>) -> Self {
        Self::Entity(v.into())
    }
}

impl From<Vec<registry_provider::Entity<EntityProperty>>> for FeathrApiResponse {
    fn from(v: Vec<registry_provider::Entity<EntityProperty>>) -> Self {
        Self::Entities(v.into_iter().collect())
    }
}

impl From<(Vec<registry_provider::Entity<EntityProperty>>, Vec<Edge>)> for FeathrApiResponse {
    fn from(v: (Vec<registry_provider::Entity<EntityProperty>>, Vec<Edge>)) -> Self {
        Self::EntityLineage(v.into())
    }
}

impl From<(Vec<Entity>, Vec<Edge>)> for FeathrApiResponse {
    fn from(v: (Vec<Entity>, Vec<Edge>)) -> Self {
        Self::EntityLineage(v.into())
    }
}

impl From<EntityLineage> for FeathrApiResponse {
    fn from(v: EntityLineage) -> Self {
        Self::EntityLineage(v)
    }
}

impl<T, E> From<Result<T, E>> for FeathrApiResponse
where
    FeathrApiResponse: From<T> + From<E>,
{
    fn from(v: Result<T, E>) -> Self {
        match v {
            Ok(t) => t.into(),
            Err(e) => e.into(),
        }
    }
}

#[async_trait]
pub trait FeathrApiProvider: Sync + Send {
    async fn request(&mut self, request: FeathrApiRequest) -> FeathrApiResponse;
}

#[async_trait]
impl<T> FeathrApiProvider for T
where
    T: RegistryProvider<EntityProperty> + Sync + Send,
{
    async fn request(&mut self, request: FeathrApiRequest) -> FeathrApiResponse {
        fn get_id<T>(t: &T, id_or_name: String) -> Result<Uuid, RegistryError>
        where
            T: RegistryProvider<EntityProperty>,
        {
            match Uuid::parse_str(&id_or_name) {
                Ok(id) => Ok(id),
                Err(_) => t.get_entity_id(&id_or_name),
            }
        }

        fn get_name<T>(t: &T, uuid: Uuid) -> Result<String, RegistryError>
        where
            T: RegistryProvider<EntityProperty>,
        {
            t.get_entity_qualified_name(uuid)
        }

        fn get_child_id<T>(
            t: &T,
            parent_id_or_name: String,
            child_id_or_name: String,
        ) -> Result<(Uuid, Uuid), RegistryError>
        where
            T: RegistryProvider<EntityProperty>,
        {
            debug!("Parent name: {}", parent_id_or_name);
            debug!("Child name: {}", child_id_or_name);
            let parent_id = get_id(t, parent_id_or_name)?;
            let child_id = match get_id(t, child_id_or_name.clone()) {
                Ok(id) => id,
                Err(_) => {
                    let project_name = get_name(t, parent_id)?;
                    get_id(t, format!("{}__{}", project_name, child_id_or_name))?
                }
            };
            Ok((parent_id, child_id))
        }

        fn search_entities<T>(
            t: &T,
            keyword: Option<String>,
            size: Option<usize>,
            offset: Option<usize>,
            types: HashSet<registry_provider::EntityType>,
            scope: Option<Uuid>,
        ) -> Result<Vec<Entity>, RegistryError>
        where
            T: RegistryProvider<EntityProperty>,
        {
            t.search_entity(
                &keyword.unwrap_or_default(),
                types,
                scope,
                size.unwrap_or(100),
                offset.unwrap_or(0),
            )
            .map(|es| es.into_iter().map(|e| fill_entity(t, e)).collect())
        }

        fn search_children<T>(
            t: &T,
            id_or_name: String,
            keyword: Option<String>,
            size: Option<usize>,
            offset: Option<usize>,
            types: HashSet<registry_provider::EntityType>,
        ) -> Result<Vec<Entity>, RegistryError>
        where
            T: RegistryProvider<EntityProperty>,
        {
            debug!("Project name: {}", id_or_name);
            let scope_id = get_id(t, id_or_name)?;

            if keyword.is_blank() {
                let children = t
                    .get_children(scope_id, types)
                    .map(|es| es.into_iter().map(|e| fill_entity(t, e)).collect());
                children.map(|mut es: Vec<_>| {
                    es.sort_by_key(|e| e.name.clone());
                    es
                })
            } else {
                search_entities(t, keyword, size, offset, types, Some(scope_id))
            }
        }

        fn fill_entity<T>(this: &T, mut e: registry_provider::Entity<EntityProperty>) -> Entity
        where
            T: RegistryProvider<EntityProperty>,
        {
            match &mut e.properties.attributes {
                registry_provider::Attributes::Project => {
                    let project_id = e.id;
                    let mut project: Entity = e.into();
                    // Contents
                    let children = this
                        .get_neighbors(project_id, EdgeType::Contains)
                        .expect("Data inconsistency detected");
                    match &mut project.attributes {
                        EntityAttributes::Project(attr) => {
                            attr.sources = children
                                .iter()
                                .filter(|&e| e.entity_type == registry_provider::EntityType::Source)
                                .map(EntityRef::new)
                                .collect();
                            attr.anchors = children
                                .iter()
                                .filter(|&e| e.entity_type == registry_provider::EntityType::Anchor)
                                .map(EntityRef::new)
                                .collect();
                            attr.anchor_features = children
                                .iter()
                                .filter(|&e| {
                                    e.entity_type == registry_provider::EntityType::AnchorFeature
                                })
                                .map(EntityRef::new)
                                .collect();
                            attr.derived_features = children
                                .iter()
                                .filter(|&e| {
                                    e.entity_type == registry_provider::EntityType::DerivedFeature
                                })
                                .map(EntityRef::new)
                                .collect();
                        }
                        _ => panic!("Data inconsistency detected"),
                    };
                    project
                }
                registry_provider::Attributes::Anchor => {
                    let anchor_id = e.id;
                    let mut anchor: Entity = e.into();
                    // Source
                    let source = this
                        .get_neighbors(anchor_id, EdgeType::Consumes)
                        .expect("Data inconsistency detected")
                        .pop()
                        .expect("Data inconsistency detected");
                    // Features
                    let features: Vec<EntityRef> = this
                        .get_neighbors(anchor_id, EdgeType::Contains)
                        .expect("Data inconsistency detected")
                        .into_iter()
                        .map(|e| EntityRef::new(&e))
                        .collect();
                    match &mut anchor.attributes {
                        EntityAttributes::Anchor(attr) => {
                            attr.source = Some(EntityRef::new(&source));
                            attr.features = features;
                        }
                        _ => panic!("Data inconsistency detected"),
                    };
                    anchor
                }
                registry_provider::Attributes::DerivedFeature(_) => {
                    let feature_id = e.id;
                    let mut feature: Entity = e.into();
                    // Contents
                    let upstream = this
                        .get_neighbors(feature_id, EdgeType::Consumes)
                        .expect("Data inconsistency detected");
                    match &mut feature.attributes {
                        EntityAttributes::DerivedFeature(attr) => {
                            attr.input_anchor_features = upstream
                                .iter()
                                .filter(|&e| {
                                    e.entity_type == registry_provider::EntityType::AnchorFeature
                                })
                                .map(EntityRef::new)
                                .collect();
                            attr.input_derived_features = upstream
                                .iter()
                                .filter(|&e| {
                                    e.entity_type == registry_provider::EntityType::DerivedFeature
                                })
                                .map(EntityRef::new)
                                .collect();
                        }
                        _ => panic!("Data inconsistency detected"),
                    };

                    feature
                }
                _ => e.into(),
            }
        }

        async fn handle_request<T>(
            this: &mut T,
            request: FeathrApiRequest,
        ) -> Result<FeathrApiResponse, ApiError>
        where
            T: RegistryProvider<EntityProperty>,
        {
            Ok(match request {
                FeathrApiRequest::GetProjects {
                    keyword,
                    size,
                    offset,
                } => if keyword.is_blank() {
                    let r = this.get_entry_points();
                    match r {
                        Ok(entities) => {
                            let mut es: Vec<Entity> = vec![];
                            for e in entities {
                                es.push(fill_entity(this, e))
                            }
                            es.sort_by_key(|e| e.name.clone());
                            Ok(es)
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    search_entities(
                        this,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::Project],
                        None,
                    )
                }
                .map(|r| {
                    r.into_iter()
                        .map(|e| e.qualified_name)
                        .collect::<Vec<String>>()
                })
                .into(),
                FeathrApiRequest::GetProject { id_or_name } => {
                    match this.get_entity_by_id_or_qualified_name(&id_or_name) {
                        Ok(e) => fill_entity(this, e).into(),
                        Err(e) => e.into(),
                    }
                }
                FeathrApiRequest::GetProjectLineage { id_or_name } => {
                    debug!("Project name: {}", id_or_name);

                    this.get_project(&id_or_name)
                        .map(|(entities, edges)| {
                            (
                                entities
                                    .into_iter()
                                    .map(|e| fill_entity(this, e))
                                    .collect::<Vec<_>>(),
                                edges,
                            )
                        })
                        .into()
                }
                FeathrApiRequest::GetProjectFeatures {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![
                            registry_provider::EntityType::AnchorFeature,
                            registry_provider::EntityType::DerivedFeature
                        ],
                    )
                    .into()
                }
                FeathrApiRequest::CreateProject { mut definition } => {
                    definition.qualified_name = definition.name.clone();
                    this.new_project(&definition.try_into()?).await.into()
                }
                FeathrApiRequest::GetProjectDataSources {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::Source],
                    )
                    .into()
                }
                FeathrApiRequest::GetProjectDataSource {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, source_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    this.get_entity(source_id)
                        .map(|e| fill_entity(this, e))
                        .into()
                }
                FeathrApiRequest::GetProjectDataSourceVersions {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, source_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    let source = this.get_entity(source_id).map(|e| fill_entity(this, e))?;
                    let mut ret = this.get_all_versions(&source.qualified_name);
                    ret.sort_by_key(|e| e.version);
                    ret.into()
                }
                FeathrApiRequest::GetProjectDataSourceVersion {
                    project_id_or_name,
                    id_or_name,
                    version,
                } => {
                    let (_, source_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    let source = this.get_entity(source_id).map(|e| fill_entity(this, e))?;
                    this.get_entity_version(&source.qualified_name, version)
                        .into()
                }
                FeathrApiRequest::CreateProjectDataSource {
                    project_id_or_name,
                    mut definition,
                } => {
                    debug!(
                        "Creating Source in project {}: {:?}",
                        project_id_or_name, definition
                    );
                    let project_id = get_id(this, project_id_or_name)?;
                    let project_name = get_name(this, project_id)?;
                    definition.qualified_name = format!("{}__{}", project_name, definition.name);
                    this.new_source(project_id, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetProjectAnchors {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::Anchor],
                    )
                    .into()
                }
                FeathrApiRequest::GetProjectAnchor {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    this.get_entity(anchor_id)
                        .map(|e| fill_entity(this, e))
                        .into()
                }
                FeathrApiRequest::GetProjectAnchorVersions {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    let anchor = this.get_entity(anchor_id).map(|e| fill_entity(this, e))?;
                    let mut ret = this.get_all_versions(&anchor.qualified_name);
                    ret.sort_by_key(|e| e.version);
                    ret.into()
                }
                FeathrApiRequest::GetProjectAnchorVersion {
                    project_id_or_name,
                    id_or_name,
                    version,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    let anchor = this.get_entity(anchor_id).map(|e| fill_entity(this, e))?;
                    this.get_entity_version(&anchor.qualified_name, version)
                        .into()
                }
                FeathrApiRequest::CreateProjectAnchor {
                    project_id_or_name,
                    mut definition,
                } => {
                    let project_id = get_id(this, project_id_or_name)?;
                    let project_name = get_name(this, project_id)?;
                    definition.qualified_name = format!("{}__{}", project_name, definition.name);
                    this.new_anchor(project_id, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetProjectDerivedFeatures {
                    project_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    debug!("Project name: {}", project_id_or_name);
                    search_children(
                        this,
                        project_id_or_name,
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::DerivedFeature],
                    )
                    .into()
                }
                FeathrApiRequest::GetProjectDerivedFeature {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, feature_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    this.get_entity(feature_id).into()
                }
                FeathrApiRequest::GetProjectDerivedFeatureVersions {
                    project_id_or_name,
                    id_or_name,
                } => {
                    let (_, feature_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    let f = this.get_entity(feature_id)?;
                    let mut ret = this.get_all_versions(&f.qualified_name);
                    ret.sort_by_key(|e| e.version);
                    ret.into()
                }
                FeathrApiRequest::GetProjectDerivedFeatureVersion {
                    project_id_or_name,
                    id_or_name,
                    version,
                } => {
                    let (_, feature_id) = get_child_id(this, project_id_or_name, id_or_name)?;
                    let f = this.get_entity(feature_id)?;
                    this.get_entity_version(&f.qualified_name, version).into()
                }
                FeathrApiRequest::CreateProjectDerivedFeature {
                    project_id_or_name,
                    mut definition,
                } => {
                    let project_id = get_id(this, project_id_or_name)?;
                    let project_name = get_name(this, project_id)?;
                    definition.qualified_name = format!("{}__{}", project_name, definition.name);
                    this.new_derived_feature(project_id, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetAnchorFeatures {
                    project_id_or_name,
                    anchor_id_or_name,
                    keyword,
                    size,
                    offset,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, anchor_id_or_name)?;
                    search_children(
                        this,
                        anchor_id.to_string(),
                        keyword,
                        size,
                        offset,
                        set![registry_provider::EntityType::AnchorFeature],
                    )
                    .into()
                }
                FeathrApiRequest::GetAnchorFeature {
                    project_id_or_name,
                    anchor_id_or_name,
                    id_or_name,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, anchor_id_or_name)?;
                    let (_, feature_id) = get_child_id(this, anchor_id.to_string(), id_or_name)?;
                    this.get_entity(feature_id).into()
                }
                FeathrApiRequest::GetAnchorFeatureVersions {
                    project_id_or_name,
                    anchor_id_or_name,
                    id_or_name,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, anchor_id_or_name)?;
                    let (_, feature_id) = get_child_id(this, anchor_id.to_string(), id_or_name)?;
                    let f = this.get_entity(feature_id)?;
                    let mut ret = this.get_all_versions(&f.qualified_name);
                    ret.sort_by_key(|e| e.version);
                    ret.into()
                }
                FeathrApiRequest::GetAnchorFeatureVersion {
                    project_id_or_name,
                    anchor_id_or_name,
                    id_or_name,
                    version,
                } => {
                    let (_, anchor_id) = get_child_id(this, project_id_or_name, anchor_id_or_name)?;
                    let (_, feature_id) = get_child_id(this, anchor_id.to_string(), id_or_name)?;
                    let f = this.get_entity(feature_id)?;
                    this.get_entity_version(&f.qualified_name, version).into()
                }
                FeathrApiRequest::CreateAnchorFeature {
                    project_id_or_name,
                    anchor_id_or_name,
                    mut definition,
                } => {
                    let (project_id, anchor_id) =
                        get_child_id(this, project_id_or_name, anchor_id_or_name)?;
                    let anchor_name = get_name(this, anchor_id)?;
                    definition.qualified_name = format!("{}__{}", anchor_name, definition.name);
                    this.new_anchor_feature(project_id, anchor_id, &definition.try_into()?)
                        .await
                        .into()
                }
                FeathrApiRequest::GetFeature { id_or_name } => this
                    .get_entity_by_id_or_qualified_name(&id_or_name)
                    .map(|e| fill_entity(this, e))
                    .into(),
                FeathrApiRequest::GetFeatureLineage { id_or_name } => {
                    debug!("Feature name: {}", id_or_name);
                    let id = get_id(this, id_or_name)?;
                    let (up_entities, up_edges) = this
                        .bfs(id, registry_provider::EdgeType::Consumes, None)
                        .map_api_error()?;
                    let (down_entities, down_edges) = this
                        .bfs(id, registry_provider::EdgeType::Produces, None)
                        .map_api_error()?;
                    (
                        up_entities
                            .into_iter()
                            .chain(down_entities.into_iter())
                            .map(|e| fill_entity(this, e))
                            .collect::<Vec<_>>(),
                        up_edges
                            .into_iter()
                            .chain(down_edges.into_iter())
                            .collect::<Vec<_>>(),
                    )
                        .into()
                }
                FeathrApiRequest::BatchLoad { entities, edges } => {
                    this.load_data(entities, edges).await.into()
                }
            })
        }

        match handle_request(self, request).await {
            Ok(v) => v,
            Err(e) => FeathrApiResponse::Error(e),
        }
    }
}
