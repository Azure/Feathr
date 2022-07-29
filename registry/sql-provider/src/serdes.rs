use registry_provider::{ToDocString, SerializableRegistry, EntityPropMutator};
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Serialize,
};
use std::{fmt::Debug, marker::PhantomData};

use crate::Registry;

impl<EntityProp> Serialize for Registry<EntityProp>
where
    EntityProp: Clone + Debug + PartialEq + Eq + ToDocString + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut entity = serializer.serialize_struct("Registry", 3)?;
        entity.serialize_field("graph", &self.graph)?;
        entity.serialize_field("deleted", &self.deleted)?;
        entity.serialize_field("permission_map", &self.permission_map.iter().collect::<Vec<_>>())?;
        entity.end()
    }
}

impl<'de, EntityProp> Deserialize<'de> for Registry<EntityProp>
where
EntityProp: Clone
+ Debug
+ PartialEq
+ Eq
+ EntityPropMutator
+ ToDocString
+ Send
+ Sync
+ Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Graph,
            Deleted,
            PermissionMap,
        }
        struct RegistryVisitor<EntityProp> {
            _t1: std::marker::PhantomData<EntityProp>,
        }

        impl<'de, EntityProp> Visitor<'de> for RegistryVisitor<EntityProp>
        where
        EntityProp: Clone
        + Debug
        + PartialEq
        + Eq
        + EntityPropMutator
        + ToDocString
        + Send
        + Sync
        + Deserialize<'de>,
        {
            type Value = Registry<EntityProp>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Registry")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Registry<EntityProp>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let graph = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let deleted = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let permission_map = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
            Ok(Registry::<EntityProp>::from_content(
                    graph, deleted, permission_map,
                ))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Registry<EntityProp>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut graph = None;
                let mut deleted = None;
                let mut permission_map = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Graph => {
                            if graph.is_some() {
                                return Err(de::Error::duplicate_field("graph"));
                            }
                            graph = Some(map.next_value()?);
                        }
                        Field::Deleted => {
                            if deleted.is_some() {
                                return Err(de::Error::duplicate_field("deleted"));
                            }
                            deleted = Some(map.next_value()?);
                        }
                        Field::PermissionMap => {
                            if permission_map.is_some() {
                                return Err(de::Error::duplicate_field("permission_map"));
                            }
                            permission_map = Some(map.next_value()?);
                        }
                    }
                }
                let graph = graph.ok_or_else(|| de::Error::missing_field("graph"))?;
                let deleted = deleted.ok_or_else(|| de::Error::missing_field("deleted"))?;
                let permission_map = permission_map.ok_or_else(|| de::Error::missing_field("permission_map"))?;
                Ok(Registry::<EntityProp>::from_content(
                    graph, deleted, permission_map,
                ))
            }
        }

        const FIELDS: &[&str] = &["graph", "deleted", "permission_map"];
        deserializer.deserialize_struct(
            "Registry",
            FIELDS,
            RegistryVisitor::<EntityProp> {
                _t1: PhantomData,
            },
        )
    }
}

impl<'de, EntityProp> SerializableRegistry<'de> for Registry<EntityProp>
where
EntityProp: Clone
+ Debug
+ PartialEq
+ Eq
+ EntityPropMutator
+ ToDocString
+ Send
+ Sync
+ Serialize
+ Deserialize<'de>,
{
    fn take_snapshot(&self) -> Result<Vec<u8>, registry_provider::RegistryError> {
        // TODO: unwrap
        Ok(serde_json::to_vec(&self).unwrap())
    }

    fn load_snapshot(&mut self, data: &'de [u8]) -> Result<(), registry_provider::RegistryError> {
        // TODO: unwrap
        *self = serde_json::from_slice::<'de, Self>(data).unwrap();
        Ok(())
    }
}