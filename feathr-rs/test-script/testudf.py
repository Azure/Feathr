from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def add_new_fare_amount(df: DataFrame) -> DataFrame:
    df = df.withColumn("fare_amount_new", col("fare_amount") + 8000000)
    return df
    