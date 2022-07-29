use common_utils::StringError;
use poem::{
    error::{BadRequest, InternalServerError},
    web::Data,
};
use poem_openapi::{
    param::{Header, Path, Query},
    payload::Json,
    OpenApi, Tags,
};
use registry_api::{
    AnchorDef, AnchorFeatureDef, ApiError, CreationResponse, DerivedFeatureDef, Entities, Entity,
    EntityLineage, FeathrApiRequest, ProjectDef, RbacResponse, SourceDef,
};
use registry_provider::{Credential, Permission};
use uuid::Uuid;

use crate::RaftRegistryApp;

#[derive(Tags)]
enum ApiTags {
    Project,
    DataSource,
    Anchor,
    AnchorFeature,
    DerivedFeature,
    Feature,
    Rbac,
}

pub struct FeathrApiV2;

#[OpenApi]
impl FeathrApiV2 {
    #[oai(path = "/projects", method = "get", tag = "ApiTags::Project")]
    async fn get_projects(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<String>>> {
        data.0
            .check_permission(credential.0, None, Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjects {
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entity_names()
            .map(Json)
    }

    #[oai(path = "/projects", method = "post", tag = "ApiTags::Project")]
    async fn new_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        def: Json<ProjectDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, None, Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        let ret = data
            .0
            .request(None, FeathrApiRequest::CreateProject { definition })
            .await
            .into_uuid_and_version();
        // Grant project admin permission to the creator of the project.
        if let Ok((uuid, _)) = &ret {
            let ret = data
                .0
                .request(
                    None,
                    FeathrApiRequest::AddUserRole {
                        project_id_or_name: uuid.to_string(),
                        user: credential.0.clone(),
                        role: Permission::Admin,
                        requestor: credential.0.clone(),
                        reason: "Created project".to_string(),
                    },
                )
                .await;
            match ret {
                registry_api::FeathrApiResponse::Error(e) => return Err(e.into()),
                _ => {}
            }
        }

        ret.map(|v| Json(v.into()))
    }

    #[oai(path = "/projects/:project", method = "get", tag = "ApiTags::Project")]
    async fn get_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProject {
                    id_or_name: project.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/lineage",
        method = "get",
        tag = "ApiTags::Project"
    )]
    async fn get_project_lineage(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectLineage {
                    id_or_name: project.0,
                },
            )
            .await
            .into_lineage()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/features",
        method = "get",
        tag = "ApiTags::Project"
    )]
    async fn get_project_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectFeatures {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasources(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSources {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "post",
        tag = "ApiTags::DataSource"
    )]
    async fn new_datasource(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        def: Json<SourceDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProjectDataSource {
                    project_id_or_name: project.0,
                    definition,
                },
            )
            .await
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/datasources/:source",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        source: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSource {
                    project_id_or_name: project.0,
                    id_or_name: source.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources/:source/versions",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        source: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSourceVersions {
                    project_id_or_name: project.0,
                    id_or_name: source.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources/:source/versions/:version",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_datasource_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        source: Path<String>,
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSourceVersion {
                    project_id_or_name: project.0,
                    id_or_name: source.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeatures {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "post",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn new_derived_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        def: Json<DerivedFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProjectDerivedFeature {
                    project_id_or_name: project.0,
                    definition,
                },
            )
            .await
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/derivedfeatures/:feature",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeature {
                    project_id_or_name: project.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/derivedfeatures/:feature/versions",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_feature_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeatureVersions {
                    project_id_or_name: project.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/derivedfeatures/:feature/versions/:version",
        method = "get",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn get_project_derived_feature_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        feature: Path<String>,
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDerivedFeatureVersion {
                    project_id_or_name: project.0,
                    id_or_name: feature.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_project_anchors(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchors {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors",
        method = "post",
        tag = "ApiTags::Anchor"
    )]
    async fn new_anchor(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        def: Json<AnchorDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateProjectAnchor {
                    project_id_or_name: project.0,
                    definition,
                },
            )
            .await
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchor {
                    project_id_or_name: project.0,
                    id_or_name: anchor.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/versions",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchorVersions {
                    project_id_or_name: project.0,
                    id_or_name: anchor.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/versions/:version",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_anchor_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchorVersion {
                    project_id_or_name: project.0,
                    id_or_name: anchor.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_anchor_features(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        keyword: Query<Option<String>>,
        size: Query<Option<usize>>,
        offset: Query<Option<usize>>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeatures {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    keyword: keyword.0,
                    size: size.0,
                    offset: offset.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "post",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn new_anchor_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        anchor: Path<String>,
        def: Json<AnchorFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Write)
            .await?;
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        data.0
            .request(
                None,
                FeathrApiRequest::CreateAnchorFeature {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    definition,
                },
            )
            .await
            .into_uuid_and_version()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features/:feature",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_project_anchor_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeature {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features/:feature/versions",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_project_anchor_feature_versions(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entities>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeatureVersions {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entities()
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features/:feature/versions/:version",
        method = "get",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn get_project_anchor_feature_version(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        anchor: Path<String>,
        feature: Path<String>,
        version: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&project), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetAnchorFeatureVersion {
                    project_id_or_name: project.0,
                    anchor_id_or_name: anchor.0,
                    id_or_name: feature.0,
                    version: parse_version(version.0)?,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(path = "/features/:feature", method = "get", tag = "ApiTags::Feature")]
    async fn get_feature(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&feature), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetFeature {
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(
        path = "/features/:feature/lineage",
        method = "get",
        tag = "ApiTags::Feature"
    )]
    async fn get_feature_lineage(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
        data.0
            .check_permission(credential.0, Some(&feature), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetFeatureLineage {
                    id_or_name: feature.0,
                },
            )
            .await
            .into_lineage()
            .map(Json)
    }

    #[oai(
        path = "/features/:feature/project",
        method = "get",
        tag = "ApiTags::Feature"
    )]
    async fn get_feature_project(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
        data.0
            .check_permission(credential.0, Some(&feature), Permission::Read)
            .await?;
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetEntityProject {
                    id_or_name: feature.0,
                },
            )
            .await
            .into_entity()
            .map(Json)
    }

    #[oai(path = "/userroles", method = "get", tag = "ApiTags::Rbac")]
    async fn get_user_roles(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
    ) -> poem::Result<Json<Vec<RbacResponse>>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Admin)
            .await?;
        data.0
            .request(opt_seq.0, FeathrApiRequest::GetUserRoles)
            .await
            .into_user_roles()
            .map(Json)
    }

    #[oai(
        path = "/users/:user/userroles/add",
        method = "post",
        tag = "ApiTags::Rbac"
    )]
    async fn add_user_role(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        user: Path<String>,
        project: Query<String>,
        role: Query<String>,
        reason: Query<String>,
    ) -> poem::Result<Json<String>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Admin)
            .await?;
        let resp = data
            .0
            .request(
                opt_seq.0,
                FeathrApiRequest::AddUserRole {
                    user: user.0.parse().map_err(|e| BadRequest(e))?,
                    project_id_or_name: project.0,
                    role: match role.0.to_lowercase().as_str() {
                        "admin" => Permission::Admin,
                        "consumer" => Permission::Read,
                        "producer" => Permission::Write,
                        _ => {
                            return Err(BadRequest(StringError::new(format!(
                                "invalid role {}",
                                role.0
                            ))))
                        }
                    },
                    requestor: credential.0.to_owned(),
                    reason: reason.0,
                },
            )
            .await;
        match resp {
            registry_api::FeathrApiResponse::Unit => Ok(Json("OK".to_string())),
            registry_api::FeathrApiResponse::Error(e) => Err(e.into()),
            _ => Err(InternalServerError(StringError::new(
                "Internal Server Error",
            ))),
        }
    }

    #[oai(
        path = "/users/:user/userroles/add",
        method = "delete",
        tag = "ApiTags::Rbac"
    )]
    async fn delete_user_role(
        &self,
        credential: Data<&Credential>,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        user: Path<String>,
        project: Query<String>,
        role: Query<String>,
        reason: Query<String>,
    ) -> poem::Result<Json<String>> {
        data.0
            .check_permission(credential.0, Some("global"), Permission::Admin)
            .await?;
        let resp = data
            .0
            .request(
                opt_seq.0,
                FeathrApiRequest::DeleteUserRole {
                    user: user.0.parse().map_err(|e| BadRequest(e))?,
                    project_id_or_name: project.0,
                    role: match role.0.to_lowercase().as_str() {
                        "admin" => Permission::Admin,
                        "consumer" => Permission::Read,
                        "producer" => Permission::Write,
                        _ => {
                            return Err(BadRequest(StringError::new(format!(
                                "invalid role {}",
                                role.0
                            ))))
                        }
                    },
                    requestor: credential.0.to_owned(),
                    reason: reason.0,
                },
            )
            .await;
        match resp {
            registry_api::FeathrApiResponse::Unit => Ok(Json("OK".to_string())),
            registry_api::FeathrApiResponse::Error(e) => Err(e.into()),
            _ => Err(InternalServerError(StringError::new(
                "Internal Server Error",
            ))),
        }
    }
}

fn parse_version<T>(v: T) -> Result<Option<u64>, ApiError>
where
    T: AsRef<str>,
{
    if v.as_ref() == "latest" {
        return Ok(None);
    }
    Ok(Some(v.as_ref().parse().map_err(|_| {
        ApiError::BadRequest(format!("Invalid version spec {}", v.as_ref()))
    })?))
}

#[cfg(test)]
mod tests {
    use super::parse_version;

    #[test]
    fn test_parse_version() {
        assert!(parse_version("").is_err());
        assert!(parse_version("xyz").is_err());
        assert!(parse_version("123xyz").is_err());
        assert!(parse_version("latest").unwrap().is_none());
        assert_eq!(parse_version("1").unwrap(), Some(1));
        assert_eq!(parse_version("42").unwrap(), Some(42));
    }
}
