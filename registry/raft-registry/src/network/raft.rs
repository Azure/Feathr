use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use poem::web::TypedHeader;
use poem::{
    handler, post,
    web::{Data, Json},
    IntoResponse, Route,
};

use crate::{ManagementCode, RaftRegistryApp, RegistryNodeId, RegistryTypeConfig};

#[handler]
pub async fn vote(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<VoteRequest<RegistryNodeId>>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;

    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[handler]
pub async fn append(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<AppendEntriesRequest<RegistryTypeConfig>>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;

    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[handler]
pub async fn snapshot(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<InstallSnapshotRequest<RegistryTypeConfig>>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;

    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}

pub fn raft_routes(route: Route) -> Route {
    route
        .at("/raft-vote", post(vote))
        .at("/raft-append", post(append))
        .at("/raft-snapshot", post(snapshot))
}
