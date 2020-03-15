import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task2b-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")
task2b=AllTrips.groupBy(AllTrips["passenger_count"].alias("number_of_passengers")).count().sort(AllTrips.passenger_count)

task=task2b.select(task2b['number_of_passengers'],task2b['count'].alias('num_trips'))
task.select(format_string("%d,%d",task.number_of_passengers,task.num_trips)).write.save("task2b-sql.out",format="text")




