use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::fmt::Debug;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AnchorDef, AnchorFeatureDef, DerivedFeatureDef, ProjectDef, RegistryError, SourceDef,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EntityType {
    Unknown,

    Project,
    Source,
    Anchor,
    AnchorFeature,
    DerivedFeature,
}

impl EntityType {
    pub fn get_name(&self) -> &'static str {
        match self {
            EntityType::Project => "feathr_workspace_v1",
            EntityType::Source => "feathr_source_v1",
            EntityType::Anchor => "feathr_anchor_v1",
            EntityType::AnchorFeature => "feathr_anchor_feature_v1",
            EntityType::DerivedFeature => "feathr_derived_feature_v1",
            EntityType::Unknown => panic!("Unknown Entity Type"),
        }
    }
}

impl EntityType {
    pub fn is_entry_point(self) -> bool {
        matches!(self, EntityType::Project)
    }
}

impl Default for EntityType {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Debug, Eq)]
pub struct Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    pub id: Uuid,
    pub entity_type: EntityType,
    pub name: String,
    pub qualified_name: String,
    pub properties: Prop,

    pub version: u64,
}

impl<Prop> Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + EntityPropMutator,
{
    pub fn set_version(&mut self, version: u64) {
        self.version = version;
        self.properties.set_version(version)
    }
}

impl<Prop> PartialEq for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<Prop> Hash for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<Prop> Serialize for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entity = serializer.serialize_struct("Entity", 6)?;
        entity.serialize_field("id", &self.id)?;
        entity.serialize_field("entity_type", &self.entity_type)?;
        entity.serialize_field("name", &self.name)?;
        entity.serialize_field("qualified_name", &self.qualified_name)?;
        entity.serialize_field("properties", &self.properties)?;
        entity.serialize_field("version", &self.version)?;
        entity.end()
    }
}

impl<'de, Prop> Deserialize<'de> for Entity<Prop>
where
    Prop: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Id,
            EntityType,
            Name,
            QualifiedName,
            Properties,
            Version,
        }
        struct EntityVisitor<T> {
            _t: std::marker::PhantomData<T>,
        }

        impl<'de, Prop> Visitor<'de> for EntityVisitor<Prop>
        where
            Prop: Clone + Debug + PartialEq + Eq + Deserialize<'de>,
        {
            type Value = Entity<Prop>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Entity")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Entity<Prop>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let id = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let entity_type = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let name = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let qualified_name = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let properties = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(4, &self))?;
                let version = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(5, &self))?;
                Ok(Entity::<Prop> {
                    id,
                    entity_type,
                    name,
                    qualified_name,
                    properties,
                    version,
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Entity<Prop>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                let mut entity_type = None;
                let mut name = None;
                let mut qualified_name = None;
                let mut properties = None;
                let mut version = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        Field::EntityType => {
                            if entity_type.is_some() {
                                return Err(de::Error::duplicate_field("entity_type"));
                            }
                            entity_type = Some(map.next_value()?);
                        }
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        Field::QualifiedName => {
                            if qualified_name.is_some() {
                                return Err(de::Error::duplicate_field("qualified_name"));
                            }
                            qualified_name = Some(map.next_value()?);
                        }
                        Field::Properties => {
                            if properties.is_some() {
                                return Err(de::Error::duplicate_field("properties"));
                            }
                            properties = Some(map.next_value()?);
                        }
                        Field::Version => {
                            if version.is_some() {
                                return Err(de::Error::duplicate_field("version"));
                            }
                            version = Some(map.next_value()?);
                        }
                    }
                }
                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let entity_type =
                    entity_type.ok_or_else(|| de::Error::missing_field("entity_type"))?;
                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let qualified_name =
                    qualified_name.ok_or_else(|| de::Error::missing_field("qualified_name"))?;
                let properties =
                    properties.ok_or_else(|| de::Error::missing_field("properties"))?;
                let version =
                    version.ok_or_else(|| de::Error::missing_field("version"))?;
                Ok(Entity::<Prop> {
                    id,
                    entity_type,
                    name,
                    qualified_name,
                    properties,
                    version,
                })
            }
        }

        const FIELDS: &[&str] = &["id", "entity_type", "name", "qualified_name", "properties", "version"];
        deserializer.deserialize_struct("Entity", FIELDS, EntityVisitor::<Prop> { _t: PhantomData })
    }
}

pub trait EntityPropMutator
where
    Self: Clone + Debug + PartialEq + Eq + crate::fts::ToDocString,
{
    fn new_project(definition: &ProjectDef) -> Result<Self, RegistryError>;
    fn new_source(definition: &SourceDef) -> Result<Self, RegistryError>;
    fn new_anchor(definition: &AnchorDef) -> Result<Self, RegistryError>;
    fn new_anchor_feature(definition: &AnchorFeatureDef) -> Result<Self, RegistryError>;
    fn new_derived_feature(definition: &DerivedFeatureDef) -> Result<Self, RegistryError>;
    fn get_version(&self) -> u64;
    fn set_version(&mut self, version: u64);
}
