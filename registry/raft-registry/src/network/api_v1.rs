use poem::web::Data;
use poem_openapi::{
    param::{Header, Path, Query},
    payload::Json,
    OpenApi, Tags,
};
use registry_api::{
    AnchorDef, AnchorFeatureDef, CreationResponse, DerivedFeatureDef, Entity, EntityLineage,
    FeathrApiRequest, ProjectDef, SourceDef,
};
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
}
pub struct FeathrApiV1;

#[OpenApi]
impl FeathrApiV1 {
    #[oai(path = "/projects", method = "get", tag = "ApiTags::Project")]
    async fn get_projects(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<String>>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjects {
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entity_names()
            .map(Json)
    }

    #[oai(path = "/projects", method = "post", tag = "ApiTags::Project")]
    async fn new_project(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        def: Json<ProjectDef>,
    ) -> poem::Result<Json<CreationResponse>> {
        let mut definition = def.0;
        if definition.id.is_empty() {
            definition.id = Uuid::new_v4().to_string();
        }
        if definition.created_by.is_empty() {
            definition.created_by = creator.0.unwrap_or_default();
        }
        data.0
            .request(None, FeathrApiRequest::CreateProject { definition })
            .await
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(path = "/projects/:project", method = "get", tag = "ApiTags::Project")]
    async fn get_project_lineage(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
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
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<Entity>>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectFeatures {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entities()
            .map(|es| es.entities)
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "get",
        tag = "ApiTags::DataSource"
    )]
    async fn get_project_datasources(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<Entity>>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectDataSources {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entities()
            .map(|es| es.entities)
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/datasources",
        method = "post",
        tag = "ApiTags::DataSource"
    )]
    async fn new_datasource(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        def: Json<SourceDef>,
    ) -> poem::Result<Json<CreationResponse>> {
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
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/derivedfeatures",
        method = "post",
        tag = "ApiTags::DerivedFeature"
    )]
    async fn new_derived_feature(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        def: Json<DerivedFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
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
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/anchors",
        method = "get",
        tag = "ApiTags::Anchor"
    )]
    async fn get_project_anchors(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        project: Path<String>,
        keyword: Query<Option<String>>,
        page: Query<Option<usize>>,
        limit: Query<Option<usize>>,
    ) -> poem::Result<Json<Vec<Entity>>> {
        data.0
            .request(
                opt_seq.0,
                FeathrApiRequest::GetProjectAnchors {
                    project_id_or_name: project.0,
                    keyword: keyword.0,
                    size: limit.0,
                    offset: page.map(|page| (page - 1) * limit.unwrap_or(10)),
                },
            )
            .await
            .into_entities()
            .map(|es| es.entities)
            .map(Json)
    }

    #[oai(
        path = "/projects/:project/anchors",
        method = "post",
        tag = "ApiTags::Anchor"
    )]
    async fn new_anchor(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        def: Json<AnchorDef>,
    ) -> poem::Result<Json<CreationResponse>> {
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
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(
        path = "/projects/:project/anchors/:anchor/features",
        method = "post",
        tag = "ApiTags::AnchorFeature"
    )]
    async fn new_anchor_feature(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-requestor")] creator: Header<Option<String>>,
        project: Path<String>,
        anchor: Path<String>,
        def: Json<AnchorFeatureDef>,
    ) -> poem::Result<Json<CreationResponse>> {
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
            .into_uuid()
            .map(|v| Json(v.into()))
    }

    #[oai(path = "/features/:feature", method = "get", tag = "ApiTags::Feature")]
    async fn get_feature(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<Entity>> {
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
        path = "/features/lineage/:feature",
        method = "get",
        tag = "ApiTags::Feature"
    )]
    async fn get_feature_lineage(
        &self,
        data: Data<&RaftRegistryApp>,
        #[oai(name = "x-registry-opt-seq")] opt_seq: Header<Option<u64>>,
        feature: Path<String>,
    ) -> poem::Result<Json<EntityLineage>> {
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
}
