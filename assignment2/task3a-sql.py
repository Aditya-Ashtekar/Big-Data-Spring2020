import sys
from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("task3a-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")
task=spark.sql('Select COUNT(fare_amount) AS Number_of_trips_with_invalid_fare_amounts from AllTrips where fare_amount <0 ')

task.select(format_string("%d",task.Number_of_trips_with_invalid_fare_amounts)).write.save("task3a-sql.out",format="text")
