from datetime import datetime
from feathrs import *
import logging
FORMAT = '%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s'
logging.basicConfig(format=FORMAT)
logging.getLogger().setLevel(logging.DEBUG)

c = FeathrClient("test-script/feathr_config.yaml")
p1 = c.new_project("feathrs_test_p1")
batch_source = p1.hdfs_source(name="nycTaxiBatchSource",
                              path="wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv",
                              timestamp_column="lpep_dropoff_datetime",
                              timestamp_column_format="yyyy-MM-dd HH:mm:ss")
print("Source id: ", batch_source.id)
request_features = p1.anchor_group("request_features",
                                   source=p1.input_context)
f_trip_distance = request_features.anchor_feature("f_trip_distance",
                                          FeatureType.FLOAT,
                                          transform="trip_distance")
f_trip_time_duration = request_features.anchor_feature("f_trip_time_duration",
                                               FeatureType.INT32,
                                               transform="(to_unix_timestamp(lpep_dropoff_datetime) - to_unix_timestamp(lpep_pickup_datetime))/60")
f_is_long_trip_distance = request_features.anchor_feature("f_is_long_trip_distance",
                                                  FeatureType.BOOLEAN,
                                                  transform="cast_float(trip_distance)>30")
f_day_of_week = request_features.anchor_feature("f_day_of_week",
                                        FeatureType.INT32,
                                        transform="dayofweek(lpep_dropoff_datetime)")

location_id = TypedKey("DOLocationID",
                       ValueType.INT32,
                       full_name="",
                       description="location id in NYC")

agg_features = p1.anchor_group("aggregationFeatures", source=batch_source)

trans_avg = Transformation.window_agg("cast_float(fare_amount)",
                                      Aggregation.AVG,
                                      "90d")
trans_max = Transformation.window_agg("cast_float(fare_amount)",
                                      Aggregation.MAX,
                                      "90d")
f_location_avg_fare = agg_features.anchor_feature("f_location_avg_fare",
                                          FeatureType.FLOAT,
                                          transform=trans_avg,
                                          keys=[location_id])
f_location_max_fare = agg_features.anchor_feature("f_location_max_fare",
                                          FeatureType.FLOAT,
                                          transform=trans_max,
                                          keys=[location_id])

f_trip_time_distance = p1.derived_feature("f_trip_time_distance",
                                  FeatureType.FLOAT,
                                  inputs=[f_trip_distance,
                                          f_trip_time_duration],
                                  transform="f_trip_distance * f_trip_time_duration")
f_trip_time_rounded = p1.derived_feature("f_trip_time_rounded",
                                 FeatureType.INT32,
                                 inputs=[f_trip_time_duration],
                                 transform="f_trip_time_duration % 10")

anchor_query = FeatureQuery(
    [
        f_trip_distance,
        f_trip_time_duration,
        f_is_long_trip_distance,
        f_day_of_week,
        f_location_avg_fare,
        f_location_max_fare,
    ],
    [location_id],
)

derived_query = FeatureQuery(
    [
        f_trip_time_distance,
        f_trip_time_rounded,
    ],
    [location_id],
)

ob = ObservationSettings("wasbs://public@azurefeathrstorage.blob.core.windows.net/sample_data/green_tripdata_2020-04.csv",
                         "lpep_dropoff_datetime",
                         "yyyy-MM-dd HH:mm:ss")

output = c.get_remote_url("output.bin")

job_id = p1.get_offline_features(
    ob, feature_query=[anchor_query, derived_query], output=output)

message = c.wait_for_job(job_id)
print(message)

features = []
start = datetime(2020, 5, 20, 0, 0, 0)
end = datetime(2020, 5, 22, 0, 0, 0)
sink = RedisSink("table1")
job_ids = p1.materialize_features([f_location_avg_fare, f_location_max_fare],
                        start,
                        end,
                        DateTimeResolution.Daily,
                        sink)
messages = c.wait_for_jobs(job_ids)
print(messages)
