import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task4a-sql").config("spark.some.config.option","some-value").getOrCreate()
Licenses_Fares = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

Licenses_Fares.createOrReplaceTempView("Licenses_Fares")

task=spark.sql('SELECT vehicle_type, COUNT(*) AS total_trips, SUM(Licenses_Fares.fare_amount) AS  total_revenue, (100*SUM(Licenses_Fares.tip_amount)/SUM(Licenses_Fares.fare_amount)) AS avg_tip_percentage FROM Licenses_Fares GROUP BY vehicle_type ORDER BY vehicle_type')

task.select(format_string("%s,%d,%.2f,%.2f",task.vehicle_type,task.total_trips,task.total_revenue,task.avg_tip_percentage)).write.save("task4a-sql.out",format="text")

