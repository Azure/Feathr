use std::{
    fs::{read_dir, remove_dir_all},
    path::PathBuf,
    pin::Pin,
    process::exit,
    vec,
};

use clap::Parser;
use common_utils::Logged;
use futures::{future::join_all, Future};
use log::{debug, info};
use poem::{
    listener::TcpListener,
    middleware::{Cors, Tracing},
    EndpointExt, Route, Server,
};
use poem_openapi::OpenApiService;
use raft_registry::{
    management_routes, raft_routes, FeathrApiV2, NodeConfig, RaftRegistryApp, RaftSequencer, FeathrApiV1,
};
use sql_provider::attach_storage;

mod spa_endpoint;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    /// Raft Node ID
    #[clap(long, env = "NODE_ID")]
    pub node_id: Option<u64>,

    /// Server Listening Address
    #[clap(long, env = "SERVER_ADDR", default_value = "0.0.0.0:8000")]
    pub http_addr: String,

    /// Reported Server Listening Address, it may differ from `http_addr` when the node is behind reversed proxy or NAT
    #[clap(long, env = "EXT_SERVER_ADDR")]
    pub ext_http_addr: Option<String>,

    /// Base Path of the API
    #[clap(long, env = "API_BASE", default_value = "/api")]
    pub api_base: String,

    /// Join the cluster via seed nodes
    #[clap(long)]
    pub seeds: Vec<String>,

    /// True to join the cluster learner as, otherwise voter
    #[clap(long)]
    pub learner: bool,

    /// True to load data from the database
    #[clap(long)]
    pub load_db: bool,

    /// True to write updates to the database
    #[clap(long)]
    pub write_db: bool,

    /// Do not init cluster when joining failed
    #[clap(long)]
    pub no_init: bool,

    #[clap(flatten)]
    pub node_config: NodeConfig,
}

/**
 * Cleanup old logs and snapshots before starting the node
 */
fn cleanup_logs(options: &Opt, node_id: u64) -> anyhow::Result<()> {
    let log_path = PathBuf::from(&options.node_config.journal_path).join(format!(
        "{}-{}.binlog",
        options.node_config.instance_prefix, node_id
    ));
    println!("Removing journal dir `{}`", log_path.to_string_lossy());
    remove_dir_all(&log_path).ok();
    std::fs::create_dir_all(&options.node_config.snapshot_path).ok();
    read_dir(&options.node_config.snapshot_path)?
        .filter(|r| {
            if let Ok(f) = r {
                f.file_type()
                    .ok()
                    .map(|ft| ft.is_file())
                    .unwrap_or_default()
                    && f.file_name()
                        .to_str()
                        .map(|f| {
                            f.starts_with(&format!(
                                "{}+{}+",
                                options.node_config.instance_prefix, node_id
                            ))
                        })
                        .unwrap_or_default()
            } else {
                false
            }
        })
        .filter_map(|f| f.ok())
        .for_each(|e| {
            println!("Removing snapshot `{}`", e.path().to_string_lossy());
            std::fs::remove_file(e.path()).ok();
        });
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    common_utils::init_logger();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    let ext_http_addr = options
        .ext_http_addr
        .clone()
        .unwrap_or_else(|| options.http_addr.clone());

    let node_config = options.node_config.clone();

    let app = if options.seeds.is_empty() {
        info!("Starting as cluster leader");
        cleanup_logs(&options, 1).ok();
        let app = RaftRegistryApp::new(1, ext_http_addr.clone(), node_config).await;
        app.init().await.ok();
        app
    } else {
        RaftRegistryApp::new(
            match options.node_id {
                Some(id) => {
                    info!("Joining cluster with node id = {}", id);
                    cleanup_logs(&options, id).ok();
                    id
                }
                None => {
                    println!("ERROR: Node ID must be specified.");
                    exit(1);
                }
            },
            ext_http_addr.clone(),
            node_config,
        )
        .await
    };

    let api_base = format!("/{}", options.api_base.trim_start_matches('/'));
    let http_addr = ext_http_addr
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string();

    let api_service_v1 = OpenApiService::new(
        FeathrApiV1,
        "Feathr Registry API Version 1",
        option_env!("CARGO_PKG_VERSION").unwrap_or("<unknown>"),
    )
    .server(&format!("http://{}{}/v1", http_addr, api_base,));
    let ui_v1 = api_service_v1.swagger_ui();
    let spec_v1 = api_service_v1.spec();

    let api_service_v2 = OpenApiService::new(
        FeathrApiV2,
        "Feathr Registry API Version 2",
        option_env!("CARGO_PKG_VERSION").unwrap_or("<unknown>"),
    )
    .server(&format!("http://{}{}/v2", http_addr, api_base,));
    let ui_v2 = api_service_v2.swagger_ui();
    let spec_v2 = api_service_v2.spec();

    let api_route = Route::new()
        .nest("/v1", api_service_v1)
        .nest("/v2", api_service_v2)
        .with(Tracing)
        .with(RaftSequencer::new(app.store.clone()))
        .with(Cors::new());
    
    let docs_route = Route::new()
        .nest("/v1", ui_v1)
        .nest("/v2", ui_v2);
    
    let spec_route = Route::new()
    .at("/v1", poem::endpoint::make_sync(move |_| spec_v1.clone()))
    .at("/v2", poem::endpoint::make_sync(move |_| spec_v2.clone()));

    let route = management_routes(raft_routes(Route::new()))
        .nest("spec", spec_route)
        .nest("docs", docs_route)
        .nest(api_base, api_route,)
        .nest(
            "/",
            spa_endpoint::SpaEndpoint::new("./static-files", "index.html"),
        )
        .data(app.clone());

    let svc_task = async {
        Server::new(TcpListener::bind(
            options.http_addr.trim_start_matches("http://"),
        ))
        .run(route)
        .await
        .log()
        .map_err(anyhow::Error::from)
    };
    let raft_task = async {
        if !options.seeds.is_empty() {
            debug!("Joining cluster");
            app.join_or_init(&options.seeds, !options.no_init)
                .await
                .log()?
        }

        if options.load_db {
            debug!("Loading data from db");
            app.load_data().await.log()?;
        }
        if options.write_db {
            // This is a writer node
            attach_storage(&mut app.store.state_machine.write().await.registry);
        }
        Ok(())
    };
    let tasks: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>>>>> = vec![Box::pin(svc_task), Box::pin(raft_task)];
    join_all(tasks.into_iter())
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(())
}
