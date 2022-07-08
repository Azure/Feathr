use poem_openapi::{Enum, Object};
use registry_provider::Edge;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Enum)]
pub enum EdgeType {
    BelongsTo,
    Contains,
    Consumes,
    Produces,
}

impl From<registry_provider::EdgeType> for EdgeType {
    fn from(v: registry_provider::EdgeType) -> Self {
        match v {
            registry_provider::EdgeType::BelongsTo => EdgeType::BelongsTo,
            registry_provider::EdgeType::Contains => EdgeType::Contains,
            registry_provider::EdgeType::Consumes => EdgeType::Consumes,
            registry_provider::EdgeType::Produces => EdgeType::Produces,
        }
    }
}

impl Into<registry_provider::EdgeType> for EdgeType {
    fn into(self) -> registry_provider::EdgeType {
        match self {
            EdgeType::BelongsTo => registry_provider::EdgeType::BelongsTo,
            EdgeType::Contains => registry_provider::EdgeType::Contains,
            EdgeType::Consumes => registry_provider::EdgeType::Consumes,
            EdgeType::Produces => registry_provider::EdgeType::Produces,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Object)]
pub struct Relationship {
    #[oai(rename = "relationshipType")]
    pub edge_type: EdgeType,
    #[oai(rename = "fromEntityId")]
    pub from: String,
    #[oai(rename = "toEntityId")]
    pub to: String,
}

impl From<Edge> for Relationship {
    fn from(v: Edge) -> Self {
        Self {
            edge_type: v.edge_type.into(),
            from: v.from.to_string(),
            to: v.to.to_string(),
        }
    }
}
