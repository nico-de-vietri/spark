# spark
batch processing

Question 1. PySpark version (1 point)
import pyspark
from pyspark.sql import SparkSession
'3.3.2'


Q2 25MB each after partitioning
!ls -lh 2024/10/*.parquet
-rw-r--r-- 1 nico nico 25M Mar  6 21:02 2024/10/part-00000-b2341259-f07b-49ac-8e23-7e4bf7f6aaeb-c000.snappy.parquet
-rw-r--r-- 1 nico nico 25M Mar  6 21:02 2024/10/part-00001-b2341259-f07b-49ac-8e23-7e4bf7f6aaeb-c000.snappy.parquet
-rw-r--r-- 1 nico nico 25M Mar  6 21:02 2024/10/part-00002-b2341259-f07b-49ac-8e23-7e4bf7f6aaeb-c000.snappy.parquet
-rw-r--r-- 1 nico nico 25M Mar  6 21:02 2024/10/part-00003-b2341259-f07b-49ac-8e23-7e4bf7f6aaeb-c000.snappy.parquet

Q3 125567
from pyspark.sql.functions import to_date

df_filtered = df.filter(to_date(df["tpep_pickup_datetime"]) == "2024-10-15")
trip_count = df_filtered.count()
print(trip_count)
128893
probably some error, 
Q4  162 hours
from pyspark.sql.functions import unix_timestamp, max

df.withColumn("trip_duration_hours", 
    (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
).agg(max("trip_duration_hours")).show()

+------------------------+
|max(trip_duration_hours)|
+------------------------+
|      162.61777777777777|
+------------------------+
Q5
!wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
df_zones = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
df_zones.createOrReplaceTempView("taxi_zones")
df.createOrReplaceTempView("yellow_trips")

result = spark.sql("""
    SELECT z.Zone, COUNT(*) AS pickup_count
    FROM yellow_trips yt
    JOIN taxi_zones z
    ON yt.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY pickup_count ASC
    LIMIT 1
""")
result.show()

+--------------------+------------+
|                Zone|pickup_count|
+--------------------+------------+
|Governor's Island...|           1|
+--------------------+------------+


