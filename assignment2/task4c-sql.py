import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task4c-sql").config("spark.some.config.option","some-value").getOrCreate()
Licenses_Fares = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

Licenses_Fares.createOrReplaceTempView("Licenses_Fares")

task=spark.sql('SELECT agent_name,SUM(fare_amount) AS revenue FROM Licenses_Fares GROUP BY agent_name ORDER BY revenue DESC LIMIT 10')

task.select(format_string("%s,%.2f",task.agent_name,task.revenue)).write.save("task4c-sql.out",format="text")





