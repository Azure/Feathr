use clap::Parser;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, Parser)]
pub struct NodeConfig {
    #[clap(
        long,
        hide = true,
        env = "RAFT_SNAPSHOT_PATH",
        default_value = "./snapshot"
    )]
    pub snapshot_path: String,

    #[clap(
        long,
        hide = true,
        env = "RAFT_INSTANCE_PREFIX",
        default_value = "feathr-registry"
    )]
    pub instance_prefix: String,

    #[clap(
        long,
        hide = true,
        env = "RAFT_JOURNAL_PATH",
        default_value = "./journal"
    )]
    pub journal_path: String,

    /// The secret to protect Raft management functions
    #[clap(long, hide = true, env = "RAFT_MANAGEMENT_CODE")]
    pub management_code: Option<String>,

    /// The Raft specific config
    #[clap(flatten)]
    pub raft_config: openraft::Config,
}
