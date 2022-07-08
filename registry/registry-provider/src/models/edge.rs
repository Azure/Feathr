use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::EntityType;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeType {
    // Feature/Source/AnchorGroup belongs to project
    BelongsTo,
    // Project Contains Feature/Source/AnchorGroup, AnchorGroup contains AnchorFeatures
    Contains,

    // AnchorGroup uses Source, DerivedFeature used Anchor/DerivedFeatures
    Consumes,
    // Source used by AnchorGroup, Anchor/DerivedFeatures derives DerivedFeature
    Produces,
}

impl Default for EdgeType {
    fn default() -> Self {
        EdgeType::BelongsTo // Whatever
    }
}

impl EdgeType {
    pub fn reflection(self) -> Self {
        match self {
            EdgeType::BelongsTo => EdgeType::Contains,
            EdgeType::Contains => EdgeType::BelongsTo,
            EdgeType::Consumes => EdgeType::Produces,
            EdgeType::Produces => EdgeType::Consumes,
        }
    }

    pub fn is_downstream(self) -> bool {
        matches!(self, EdgeType::Contains | EdgeType::Produces)
    }

    pub fn is_upstream(self) -> bool {
        matches!(self, EdgeType::BelongsTo | EdgeType::Consumes)
    }

    pub fn validate(&self, from: EntityType, to: EntityType) -> bool {
        matches!(
            (from, to, self),
            (EntityType::Project, EntityType::Source, EdgeType::Contains)
                | (EntityType::Project, EntityType::Anchor, EdgeType::Contains)
                | (
                    EntityType::Project,
                    EntityType::AnchorFeature,
                    EdgeType::Contains
                )
                | (
                    EntityType::Project,
                    EntityType::DerivedFeature,
                    EdgeType::Contains
                )
                | (EntityType::Source, EntityType::Project, EdgeType::BelongsTo)
                | (EntityType::Source, EntityType::Anchor, EdgeType::Produces)
                | (
                    EntityType::Source,
                    EntityType::AnchorFeature,
                    EdgeType::Produces
                )
                | (EntityType::Anchor, EntityType::Project, EdgeType::BelongsTo)
                | (EntityType::Anchor, EntityType::Source, EdgeType::Consumes)
                | (
                    EntityType::Anchor,
                    EntityType::AnchorFeature,
                    EdgeType::Contains
                )
                | (
                    EntityType::AnchorFeature,
                    EntityType::Project,
                    EdgeType::BelongsTo
                )
                | (
                    EntityType::AnchorFeature,
                    EntityType::Source,
                    EdgeType::Consumes
                )
                | (
                    EntityType::AnchorFeature,
                    EntityType::Anchor,
                    EdgeType::BelongsTo
                )
                | (
                    EntityType::AnchorFeature,
                    EntityType::DerivedFeature,
                    EdgeType::Produces
                )
                | (
                    EntityType::DerivedFeature,
                    EntityType::Project,
                    EdgeType::BelongsTo
                )
                | (
                    EntityType::DerivedFeature,
                    EntityType::AnchorFeature,
                    EdgeType::Consumes
                )
                | (
                    EntityType::DerivedFeature,
                    EntityType::DerivedFeature,
                    EdgeType::Produces
                )
                | (
                    EntityType::DerivedFeature,
                    EntityType::DerivedFeature,
                    EdgeType::Consumes
                )
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Edge
{
    #[serde(rename = "relationshipType")]
    pub edge_type: EdgeType,
    #[serde(rename = "fromEntityId")]
    pub from: Uuid,
    #[serde(rename = "toEntityId")]
    pub to: Uuid,
}

impl Edge
{
    pub fn reflection(&self) -> Self {
        Self {
            from: self.to,
            to: self.from,
            edge_type: self.edge_type.reflection(),
        }
    }
}
