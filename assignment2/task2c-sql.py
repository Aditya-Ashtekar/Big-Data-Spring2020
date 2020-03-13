import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task2c-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")

task2c=spark.sql('SELECT DATE(pickup_datetime) as date, ROUND(SUM(fare_amount),2) AS value1,ROUND(SUM(tip_amount),2) AS value2,ROUND(SUM(surcharge),2) AS value3,ROUND(SUM(tolls_amount),2) AS total_tolls FROM AllTrips GROUP BY DATE(pickup_datetime) ORDER BY date')
task2c.createOrReplaceTempView("task2c")
task=spark.sql('SELECT date,ROUND((value1+value2+value3),2) AS total_revenue,total_tolls FROM task2c')
task.select(format_string("%s,%.2f, %.2f",date_format(task.date,'yyyy-mm-dd'),task.total_revenue,task.total_tolls)).write.save("task2c-sql.out",format="text")



