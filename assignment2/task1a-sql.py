import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1a-sql").config("spark.some.config.option","some-value").getOrCreate()
trips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
fares = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
trips.createOrReplaceTempView("trips")
fares.createOrReplaceTempView("fares")

AllTrips= trips.join(fares,(fares.medallion==trips.medallion ) & (fares.hack_license==trips.hack_license) &(fares.vendor_id == trips.vendor_id)&(fares.pickup_datetime==trips.pickup_datetime)).select(fares["medallion"],fares["hack_license"],fares["vendor_id"],fares["pickup_datetime"],trips["rate_code"],trips["store_and_fwd_flag"],trips["dropoff_datetime"],trips["passenger_count"],trips["trip_time_in_secs"],trips["trip_distance"],trips["pickup_longitude"],trips["pickup_latitude"],trips["dropoff_longitude"],trips["dropoff_latitude"],fares["payment_type"],fares["fare_amount"],fares["surcharge"],fares["mta_tax"],fares["tip_amount"],fares["tolls_amount"],fares["total_amount"])

AllTrips.orderBy('medallion','hack_license','pickup_datetime')

AllTrips.select(AllTrips.medallion,AllTrips.hack_license,AllTrips.vendor_id,AllTrips.pickup_datetime,AllTrips.rate_code,AllTrips.store_and_fwd_flag,AllTrips.dropoff_datetime,AllTrips.passenger_count,AllTrips.trip_time_in_secs,AllTrips.trip_distance,AllTrips.pickup_longitude,AllTrips.pickup_latitude,AllTrips.dropoff_longitude,AllTrips.dropoff_latitude,AllTrips.payment_type,AllTrips.fare_amount,AllTrips.surcharge,AllTrips.mta_tax,AllTrips.tip_amount,AllTrips.tolls_amount,AllTrips.total_amount).write.option("header","true").csv("task1a-sql.out")


