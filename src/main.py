import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import to_date, count

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField('dispatching_base_num', StringType(), True),
    StructField('pickup_datetime', TimestampType(), True),
    StructField('dropOff_datetime', TimestampType(), True),
    StructField('PUlocationID', IntegerType(), True),
    StructField('DOlocationID', IntegerType(), True),
    StructField('SR_Flag', StringType(), True),
    StructField('Affiliated_base_number', StringType(), True)
])
df = spark.read.csv('./data/fhv_tripdata_2019-10.csv', header=True, schema=schema)

#average size of the Parquet
df = df.repartition(6)
df.write.parquet('./data_partitioned/fhv_tripdata/2019/10')

#trips that started on the 15th of October
print(df.filter(to_date(df.pickup_datetime) == '2019-10-15').count())

#longest trip
df = df.withColumn('trip_length', df.dropOff_datetime - df.pickup_datetime)
print(df.agg({'trip_length': 'max'}).collect()[0][0])

#LEAST frequent pickup location Zone
df_zones = spark.read.csv('./data/taxi_zone_lookup.csv', header=True)
df_with_zones = df.join(df_zones, df.PUlocationID == df_zones.LocationID, how='left')
print(df_with_zones.groupby('Zone').agg(count('Zone').alias('n_trip')).orderBy('n_trip').show())

