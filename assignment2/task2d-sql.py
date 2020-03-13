import sys
from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("task2d-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")

task=spark.sql('Select medallion,COUNT(*) AS number_of_trips, COUNT(DISTINCT DATE(pickup_datetime)) AS days_driven, ROUND(COUNT(*)/COUNT(DISTINCT DATE(pickup_datetime)),2) AS average from AllTrips GROUP BY medallion ORDER by medallion')

task.select(format_string("%s,%d,%d,%.2f",task.medallion,task.number_of_trips,task.days_driven,task.average)).write.save("task2d-sql.out",format="text")
