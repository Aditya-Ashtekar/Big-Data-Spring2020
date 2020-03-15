import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task3c-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")
task=spark.sql('SELECT medallion AS tm, COUNT(*) AS no_gps_trips_count FROM AllTrips WHERE pickup_longitude = 0 AND pickup_latitude = 0 AND dropoff_longitude = 0 AND dropoff_latitude = 0  GROUP BY medallion')
task1=spark.sql('SELECT medallion, COUNT(*) AS all_trips_count FROM AllTrips GROUP BY medallion')

task1.createOrReplaceTempView("task1")
task.createOrReplaceTempView("task")
result=task.join(task1, task.tm == task1.medallion,how='right')
result.createOrReplaceTempView("result")
task_output=spark.sql('SELECT medallion, (100 * first(IFNULL(no_gps_trips_count,0) / all_trips_count)) AS percentage_of_trips FROM result GROUP BY medallion ORDER BY medallion')
task_output.select(format_string("%s,%.2f",task_output.medallion,task_output.percentage_of_trips)).write.save("task3c-sql.out",format="text")



