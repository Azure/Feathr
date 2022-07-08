use std::{path::Path, sync::Arc, collections::HashMap};

use chrono::Duration;
use futures::future::join_all;
use log::{debug, warn};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    job_client, load_var_source, new_var_source, Error, FeathrApiClient, FeathrProject, JobClient,
    JobId, JobStatus, SubmitJobRequest, VarSource, FeatureRegistry, registry_client::api_models, project::FeathrProjectImpl,
};

#[derive(Clone, Debug)]
pub struct FeathrClient {
    pub(crate) inner: Arc<FeathrClientImpl>,
}

impl FeathrClient {
    pub async fn load<T>(conf_file: T) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        FeathrClientImpl::load(conf_file).await.map(|inner| Self {
            inner: Arc::new(inner),
        })
    }

    pub async fn from_str(content: &str) -> Result<Self, Error> {
        FeathrClientImpl::from_str(content).await.map(|inner| Self {
            inner: Arc::new(inner),
        })
    }

    pub async fn load_project(&self, name: &str) -> Result<FeathrProject, Error> {
        if let Some(r) = self.inner.get_registry_client() {
            let lineage = r.load_project(name).await?;
            let mut project: FeathrProjectImpl = lineage.try_into()?;
            // Set owner
            project.owner = Some(self.inner.clone());
            Ok(FeathrProject {
                inner: Arc::new(RwLock::new(project)),
            })
        } else {
            Err(Error::DetachedClient)
        }
    }

    pub async fn new_project(&self, name: &str) -> Result<FeathrProject, Error> {
        self.new_project_with_tags(name, Default::default()).await
    }

    pub async fn new_project_with_tags(&self, name: &str, tags: HashMap<String, String>) -> Result<FeathrProject, Error> {
        let id = if let Some(r) = self.inner.get_registry_client() {
            let def = api_models::ProjectDef {
                name: name.to_string(),
                tags,
            };
            r.new_project(def).await?
        } else {
            warn!("The project {} is created in detached mode, all changes will not be able to be stored to the registry", name);
            Uuid::new_v4()
        };
        Ok(FeathrProject::new(self.inner.clone(), name, id).await)
    }

    pub async fn submit_job(&self, request: SubmitJobRequest) -> Result<JobId, Error> {
        self.inner.submit_job(request).await
    }

    pub async fn submit_jobs(&self, requests: Vec<SubmitJobRequest>) -> Result<Vec<JobId>, Error> {
        self.inner.submit_jobs(requests).await
    }

    pub async fn wait_for_job(
        &self,
        job_id: JobId,
        timeout: Option<Duration>,
    ) -> Result<String, Error> {
        self.inner.wait_for_job(job_id, timeout).await
    }

    pub async fn wait_for_jobs(
        &self,
        job_ids: Vec<JobId>,
        timeout: Option<Duration>,
    ) -> Vec<Result<String, Error>> {
        self.inner.wait_for_jobs(job_ids, timeout).await
    }

    pub fn get_remote_url(&self, path: &str) -> String {
        self.inner.get_remote_url(path)
    }

    pub async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, Error> {
        self.inner.get_job_status(job_id).await
    }

    pub async fn get_job_output_url(&self, job_id: JobId) -> Result<Option<String>, crate::Error> {
        self.inner.get_job_output_url(job_id).await
    }
}

#[derive(Clone, Debug)]
pub struct FeathrClientImpl {
    job_client: job_client::Client,
    registry_client: Option<Arc<FeathrApiClient>>,
    var_source: Arc<dyn VarSource + Send + Sync>,
}

impl FeathrClientImpl {
    pub async fn load<T>(conf_file: T) -> Result<Self, Error>
    where
        T: AsRef<Path>,
    {
        let var_source = load_var_source(conf_file);
        Ok(Self {
            job_client: job_client::Client::from_var_source(var_source.clone()).await?,
            registry_client: FeathrApiClient::from_var_source(var_source.clone())
                .await
                .ok()
                .map(Arc::new),
            var_source,
        })
    }

    pub async fn from_str(content: &str) -> Result<Self, Error> {
        let var_source = new_var_source(content);
        Ok(Self {
            job_client: job_client::Client::from_var_source(var_source.clone()).await?,
            registry_client: FeathrApiClient::from_var_source(var_source.clone())
                .await
                .ok()
                .map(Arc::new),
            var_source,
        })
    }

    pub fn get_registry_client(&self) -> Option<Arc<FeathrApiClient>> {
        self.registry_client.clone()
    }

    pub async fn submit_job(&self, request: SubmitJobRequest) -> Result<JobId, Error> {
        self.job_client
            .submit_job(self.var_source.clone(), request)
            .await
    }

    pub async fn submit_jobs(&self, requests: Vec<SubmitJobRequest>) -> Result<Vec<JobId>, Error> {
        let mut ret = vec![];
        for request in requests.into_iter() {
            ret.push(
                self.job_client
                    .submit_job(self.var_source.clone(), request)
                    .await?,
            )
        }
        Ok(ret)
    }

    pub async fn wait_for_job(
        &self,
        job_id: JobId,
        timeout: Option<Duration>,
    ) -> Result<String, Error> {
        let status = self.job_client.wait_for_job(job_id, timeout).await?;
        debug!("Job {} completed with status {}", job_id, status);
        self.job_client.get_job_log(job_id).await
    }

    pub async fn wait_for_jobs(
        &self,
        job_ids: Vec<JobId>,
        timeout: Option<Duration>,
    ) -> Vec<Result<String, Error>> {
        let jobs = job_ids
            .into_iter()
            .map(|job_id| self.wait_for_job(job_id, timeout));
        let complete = join_all(jobs).await;
        complete
    }

    pub async fn get_job_status(&self, job_id: JobId) -> Result<JobStatus, Error> {
        self.job_client.get_job_status(job_id).await
    }

    pub fn get_remote_url(&self, path: &str) -> String {
        self.job_client.get_remote_url(path)
    }

    pub async fn get_job_output_url(&self, job_id: JobId) -> Result<Option<String>, crate::Error> {
        self.job_client.get_job_output_url(job_id).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use futures::future::join_all;

    use crate::*;

    async fn init() -> FeathrClient {
        crate::tests::init_logger();
        FeathrClient::load("test-script/feathr_config.yaml")
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn materialization_e2e_job() {
        let client = init().await;
        let proj = FeathrProject::new_detached("p1").await;
        let batch_source = proj.hdfs_source("nycTaxiBatchSource", "wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv")
            .time_window(
                "lpep_dropoff_datetime",
                "yyyy-MM-dd HH:mm:ss"
            )
            .build()
            .await
            .unwrap();

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let trans = Transformation::window_agg(
            "cast_float(fare_amount)",
            Aggregation::AVG,
            Duration::days(90),
        )
        .unwrap();

        let agg_features = proj
            .anchor_group("aggregationFeatures", batch_source)
            .build()
            .await
            .unwrap();

        let f_location_avg_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(&trans)
            .build()
            .await
            .unwrap();

        let f_location_max_fare = agg_features
            .anchor("f_location_max_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(trans)
            .build()
            .await
            .unwrap();

        let start = Utc.ymd(2020, 5, 20).and_hms(0, 0, 0);
        let reqs = proj
            .feature_gen_job(
                &[&f_location_avg_fare, &f_location_max_fare],
                start,
                start + Duration::days(3),
                DateTimeResolution::Daily,
            )
            .await
            .unwrap()
            .sink(RedisSink::new("table1"))
            .build()
            .unwrap();
        for r in reqs.iter() {
            println!("{}:\n{}", r.job_config_file_name, r.gen_job_config);
        }

        let job_ids = client.submit_jobs(reqs).await.unwrap();

        let finished = job_ids.iter().map(|&id| client.wait_for_job(id, None));
        let outputs: Vec<String> = join_all(finished)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        println!("{:#?}", outputs);

        for id in job_ids.into_iter() {
            assert_eq!(client.get_job_status(id).await.unwrap(), JobStatus::Success);
        }
    }

    #[tokio::test]
    async fn join_e2e_job() {
        let client = init().await;
        let proj = client.new_project("p1").await.unwrap();
        let batch_source = proj.hdfs_source("nycTaxiBatchSource", "wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv")
            .time_window(
                "lpep_dropoff_datetime",
                "yyyy-MM-dd HH:mm:ss"
            )
            .preprocessing("testudf.add_new_fare_amount")
            .build()
            .await
            .unwrap();

        let request_features = proj
            .anchor_group("request_features", proj.INPUT_CONTEXT().await)
            .build()
            .await
            .unwrap();

        let f_trip_distance = request_features
            .anchor("f_trip_distance", FeatureType::FLOAT)
            .unwrap()
            .transform("trip_distance")
            .build()
            .await
            .unwrap();

        let f_trip_time_duration = request_features
            .anchor("f_trip_time_duration", FeatureType::INT32)
            .unwrap()
            .transform("(to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime))/60")
            .build()
            .await
            .unwrap();

        let f_is_long_trip_distance = request_features
            .anchor("f_is_long_trip_distance", FeatureType::BOOLEAN)
            .unwrap()
            .transform("cast_float(trip_distance)>30")
            .build()
            .await
            .unwrap();

        let f_day_of_week = request_features
            .anchor("f_day_of_week", FeatureType::INT32)
            .unwrap()
            .transform("dayofweek(lpep_dropoff_datetime)")
            .build()
            .await
            .unwrap();

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let agg_features = proj
            .anchor_group("aggregationFeatures", batch_source.clone())
            .build()
            .await
            .unwrap();

        let trans = Transformation::window_agg(
            "cast_float(fare_amount)",
            Aggregation::AVG,
            Duration::days(90),
        )
        .unwrap();

        let f_location_avg_fare = agg_features
            .anchor("f_location_avg_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(&trans)
            .build()
            .await
            .unwrap();

        let f_location_max_fare = agg_features
            .anchor("f_location_max_fare", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&location_id])
            .transform(trans)
            .build()
            .await
            .unwrap();

        let f_trip_time_distance = proj
            .derived_feature("f_trip_time_distance", FeatureType::FLOAT)
            .add_input(&f_trip_distance)
            .add_input(&f_trip_time_duration)
            .transform("f_trip_distance * f_trip_time_duration")
            .build()
            .await
            .unwrap();

        let f_trip_time_rounded = proj
            .derived_feature("f_trip_time_rounded", FeatureType::INT32)
            .add_input(&f_trip_time_duration)
            .transform("f_trip_time_duration % 10")
            .build()
            .await
            .unwrap();

        let pickup_time_as_id = TypedKey::new("lpep_pickup_datetime", ValueType::INT32)
            .full_name("nyc_taxi.pickup_time_as_id")
            .description("Pick up time");

        let udf_features = proj
            .anchor_group("udfFeatures", batch_source)
            .build()
            .await
            .unwrap();

        let fare_amount_new = udf_features
            .anchor("fare_amount_new", FeatureType::FLOAT)
            .unwrap()
            .keys(&[&pickup_time_as_id])
            .transform("fare_amount_new")
            .build()
            .await
            .unwrap();

        println!("features.conf:\n{}", proj.get_feature_config().await.unwrap());

        let output = client.get_remote_url("output.bin");
        let anchor_query = FeatureQuery::new(
            &[
                &f_trip_distance,
                &f_trip_time_duration,
                &f_is_long_trip_distance,
                &f_day_of_week,
                &f_location_avg_fare,
                &f_location_max_fare,
                &fare_amount_new,
            ],
            &[&location_id],
        );
        let derived_query = FeatureQuery::new(
            &[&f_trip_time_distance, &f_trip_time_rounded],
            &[&location_id],
        );
        let ob = ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss");

        println!(
            "features_join.conf:\n{}",
            proj.get_feature_join_config(&ob, &[&anchor_query, &derived_query], &output)
                .unwrap()
        );

        let req = proj
            .feature_join_job(&ob, &[&anchor_query, &derived_query], &output)
            .await
            .unwrap()
            .python_file("test-script/testudf.py")
            .output_path(&output)
            .build();

        println!("Request: {:#?}", req);

        let id = client.submit_job(req).await.log().unwrap();

        let log = client.wait_for_job(id, None).await.unwrap();

        println!("Job output:\n{}", log);

        println!(
            "Job output URL: {}",
            client
                .get_job_output_url(id)
                .await
                .ok()
                .flatten()
                .unwrap_or_default()
        );

        assert_eq!(client.get_job_status(id).await.unwrap(), JobStatus::Success);
    }

    #[tokio::test]
    #[ignore = "Rely on registry"]
    async fn test_load() {
        let client = init().await;
        let proj = client.load_project("p1").await.unwrap();
        println!("features.conf:\n{}", proj.get_feature_config().await.unwrap());

        let location_id = TypedKey::new("DOLocationID", ValueType::INT32)
            .full_name("nyc_taxi.location_id")
            .description("location id in NYC");

        let output = client.get_remote_url("output.bin");
        let anchor_query = FeatureQuery::new(
            &[
                "f_trip_distance",
                "f_trip_time_duration",
                "f_is_long_trip_distance",
                "f_day_of_week",
                "f_location_avg_fare",
                "f_location_max_fare",
                "fare_amount_new",
            ],
            &[&location_id],
        );
        let derived_query = FeatureQuery::new(
            &["f_trip_time_distance", "f_trip_time_rounded"],
            &[&location_id],
        );
        let ob = ObservationSettings::new("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv", "lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss");

        println!(
            "features_join.conf:\n{}",
            proj.get_feature_join_config(&ob, &[&anchor_query, &derived_query], &output)
                .unwrap()
        );

        let req = proj
            .feature_join_job(&ob, &[&anchor_query, &derived_query], &output)
            .await
            .unwrap()
            .python_file("test-script/testudf.py")
            .output_path(&output)
            .build();

        println!("Request: {:#?}", req);

        let id = client.submit_job(req).await.log().unwrap();

        let log = client.wait_for_job(id, None).await.unwrap();

        println!("Job output:\n{}", log);

        println!(
            "Job output URL: {}",
            client
                .get_job_output_url(id)
                .await
                .ok()
                .flatten()
                .unwrap_or_default()
        );

        assert_eq!(client.get_job_status(id).await.unwrap(), JobStatus::Success);
    }
}
