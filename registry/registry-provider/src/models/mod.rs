mod entity;
mod edge;
mod attributes;
mod entity_prop;
mod entity_def;

pub use entity::*;
pub use edge::*;
pub use attributes::*;
pub use entity_prop::*;
pub use entity_def::*;

pub const PROJECT_TYPE: &str = "feathr_workspace_v1";
pub const ANCHOR_TYPE: &str = "feathr_anchor_v1";
pub const ANCHOR_FEATURE_TYPE: &str = "feathr_anchor_feature_v1";
pub const DERIVED_FEATURE_TYPE: &str = "feathr_derived_feature_v1";
pub const SOURCE_TYPE: &str = "feathr_source_v1";


#[cfg(test)]
mod tests {
    use crate::{models::*, Entity};

    #[test]
    fn des_trans() {
        let s = r#"{
            "filter": null,
            "agg_func": "AVG",
            "limit": null,
            "group_by": null,
            "window": "90d",
            "def_expr": "cast_float(fare_amount)"
        }"#;

        let t: FeatureTransformation = serde_json::from_str(s).unwrap();
        println!("{:#?}", t);
    }

    #[test]
    fn des_derived() {
        let s = r#"{
            "typeName": "feathr_derived_feature_v1",
            "attributes": {
                "qualifiedName": "feathr_ci_registry_12_33_182947__f_trip_time_rounded",
                "name": "f_trip_time_rounded",
                "input_derived_features": [],
                "type": "\n            type: {\n                type: TENSOR\n                tensorCategory: DENSE\n                dimensionType: []\n                valType: INT\n            }\n        ",
                "transformation": {
                    "transform_expr": "f_trip_time_duration % 10"
                },
                "input_anchor_features": [
                    {
                        "guid": "103baca1-377a-4ddf-8429-5da91026c269",
                        "typeName": "feathr_anchor_feature_v1",
                        "uniqueAttributes": {
                            "qualifiedName": "feathr_ci_registry_12_33_182947__request_features__f_trip_time_duration"
                        }
                    }
                ],
                "key": [
                    {
                        "full_name": "feathr.dummy_typedkey",
                        "key_column": "NOT_NEEDED",
                        "description": "A dummy typed key for passthrough/request feature.",
                        "key_column_alias": "NOT_NEEDED",
                        "key_column_type": "0"
                    }
                ]
            },
            "lastModifiedTS": "1",
            "guid": "c626c41c-d6c2-4b16-a267-6cdeea497c52",
            "status": "ACTIVE",
            "displayText": "f_trip_time_rounded",
            "classificationNames": [],
            "meaningNames": [],
            "meanings": [],
            "isIncomplete": false,
            "labels": []
        }"#;

        let e: EntityProperty = serde_json::from_str(s).unwrap();
        let e: Entity<EntityProperty> = e.into();
        println!("{:#?}", e);
    }

    #[test]
    fn des_entity() {
        let s = r#"{
            "typeName": "feathr_anchor_feature_v1",
            "attributes": {
                "qualifiedName": "feathr_ci_registry_12_33_182947__aggregationFeatures__f_location_avg_fare",
                "name": "f_location_avg_fare",
                "type": "\n            type: {\n                type: TENSOR\n                tensorCategory: DENSE\n                dimensionType: []\n                valType: FLOAT\n            }\n        ",
                "transformation": {
                    "filter": null,
                    "agg_func": "AVG",
                    "limit": null,
                    "group_by": null,
                    "window": "90d",
                    "def_expr": "cast_float(fare_amount)"
                },
                "key": [
                    {
                        "full_name": "nyc_taxi.location_id",
                        "key_column": "DOLocationID",
                        "description": "location id in NYC",
                        "key_column_alias": "DOLocationID",
                        "key_column_type": "2"
                    }
                ]
            },
            "lastModifiedTS": "1",
            "guid": "2a052ccd-3e31-46a7-bffb-2ab1302b1b00",
            "status": "ACTIVE",
            "displayText": "f_location_avg_fare",
            "classificationNames": [],
            "meaningNames": [],
            "meanings": [],
            "isIncomplete": false,
            "labels": []
        }"#;

        let e: EntityProperty = serde_json::from_str(s).unwrap();
        let e: Entity<EntityProperty> = e.into();
        println!("{:#?}", e);
    }
}
