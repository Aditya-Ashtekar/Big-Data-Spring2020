import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task3b-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")
task=task=spark.sql('SELECT medallion, pickup_datetime FROM AllTrips GROUP BY medallion, pickup_datetime HAVING COUNT(*) > 1 ORDER BY medallion,pickup_datetime')

task.select(format_string("%s,%s",task.medallion,date_format(task.pickup_datetime,'yyyy-mm-dd'))).write.save("task3b-sql.out",format="text")


