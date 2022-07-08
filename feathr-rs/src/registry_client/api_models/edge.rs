use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeType {
    BelongsTo,
    Contains,
    Consumes,
    Produces,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Relationship {
    #[serde(rename = "relationshipType")]
    pub edge_type: EdgeType,
    #[serde(rename = "fromEntityId")]
    pub from: Uuid,
    #[serde(rename = "toEntityId")]
    pub to: Uuid,
}
