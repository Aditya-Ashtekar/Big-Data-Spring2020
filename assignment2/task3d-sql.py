import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task3d-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")
task=spark.sql('SELECT hack_license, COUNT(DISTINCT medallion) AS num_taxis_used FROM AllTrips GROUP BY hack_license ORDER BY hack_license')
task.select(format_string("%s,%d",task.hack_license,task.num_taxis_used)).write.save("task3d-sql.out",format="text")




