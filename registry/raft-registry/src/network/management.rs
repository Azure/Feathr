use std::collections::{BTreeMap, BTreeSet};

use openraft::{
    error::{CheckIsLeaderError, Infallible},
    raft::ClientWriteRequest,
    EntryPayload, Node, RaftMetrics,
};
use poem::{
    get, handler, post,
    web::{Data, Json, TypedHeader},
    IntoResponse, Route,
};
use poem_openapi::payload::PlainText;
use registry_api::{ApiError, FeathrApiProvider, FeathrApiRequest, FeathrApiResponse};
use reqwest::StatusCode;

use crate::{ManagementCode, RaftRegistryApp, RegistryNodeId, RegistryTypeConfig};

#[handler]
pub async fn add_learner(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<(RegistryNodeId, String)>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;
    let node_id = req.0 .0;
    let node = Node {
        addr: req.0 .1.clone(),
        ..Default::default()
    };
    let res = app.raft.add_learner(node_id, Some(node), true).await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[handler]
pub async fn change_membership(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<BTreeSet<RegistryNodeId>>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;
    let res = app.raft.change_membership(req.0, true, false).await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[handler]
pub async fn init(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        Node {
            addr: app.addr.clone(),
            data: Default::default(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

/// Get the latest metrics of the cluster
#[handler]
pub async fn metrics(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<RegistryTypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(res))
}

/**
 * Handle request locally, may get stale response
 */
#[handler]
pub async fn handle_request(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<FeathrApiRequest>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;

    if req.0.is_writing_request() {
        return Err(ApiError::BadRequest(
            "Updating requests must be submitted to the Raft leader".to_string(),
        ))?;
    }

    let value = app
        .store
        .state_machine
        .write()
        .await
        .registry
        .request(req.0)
        .await;
    let res: Result<FeathrApiResponse, Infallible> = Ok(value);
    Ok(Json(res))
}

/**
 * Handle request only if this node is the leader, return error otherwise
 */
#[handler]
pub async fn handle_leader_request(
    app: Data<&RaftRegistryApp>,
    code: Option<TypedHeader<ManagementCode>>,
    req: Json<FeathrApiRequest>,
) -> poem::Result<impl IntoResponse> {
    app.check_code(code.map(|c| c.0)).await?;

    let ret = app.raft.is_leader().await;
    match ret {
        Ok(_) => {
            // Only writing requests need to go to raft state machine
            let value = if req.0.is_writing_request() {
                let request = ClientWriteRequest::new(EntryPayload::Normal(req.0));
                app.raft
                    .client_write(request)
                    .await
                    .map_err(|e| ApiError::InternalError(format!("{:?}", e)))?
                    .data
            } else {
                app.store
                    .state_machine
                    .write()
                    .await
                    .registry
                    .request(req.0)
                    .await
            };
            let res: Result<FeathrApiResponse, CheckIsLeaderError<RegistryNodeId>> = Ok(value);
            Ok(Json(res))
        }
        Err(e) => Ok(Json(Err(e))),
    }
}

/**
 * Check if the program is still alive
 */
#[handler]
pub fn liveness() -> poem::Result<impl IntoResponse> {
    Ok(PlainText("OK"))
}

/**
 * Check if the node is in a good state
 */
#[handler]
pub async fn readiness(app: Data<&RaftRegistryApp>) -> poem::Result<impl IntoResponse> {
    let m = app.raft.metrics().borrow().clone();
    Ok(
        if m.running_state.is_ok() && m.current_leader.is_some() && m.last_applied.is_some() {
            PlainText("OK").with_status(StatusCode::OK).into_response()
        } else {
            PlainText("Not Ok")
                .with_header("Retry-After", 5)
                .with_status(StatusCode::SERVICE_UNAVAILABLE)
                .into_response()
        },
    )
}

pub fn management_routes(route: Route) -> Route {
    route
        .at("/add-learner", post(add_learner))
        .at("/change-membership", post(change_membership))
        .at("/init", post(init))
        .at("/metrics", get(metrics))
        .at("/handle-request", post(handle_request))
        .at("/handle-leader-request", post(handle_leader_request))
        .at("/ping", get(liveness))
        .at("/ready", get(readiness))
}
