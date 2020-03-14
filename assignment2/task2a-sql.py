import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task2a-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")

task1=spark.sql('SELECT COUNT(fare_amount) AS amount_range FROM AllTrips WHERE fare_amount>0 AND fare_amount<=5')
task2=spark.sql('SELECT COUNT(fare_amount) AS amount_range FROM AllTrips WHERE fare_amount>5 AND fare_amount<=15')
task3=spark.sql('SELECT COUNT(fare_amount) AS amount_range FROM AllTrips WHERE fare_amount>15 AND fare_amount<=30')
task4=spark.sql('SELECT COUNT(fare_amount) AS amount_range FROM AllTrips WHERE fare_amount>30 AND fare_amount<=50')
task5=spark.sql('SELECT COUNT(fare_amount) AS amount_range FROM AllTrips WHERE fare_amount>50 AND fare_amount<=100')
task6=spark.sql('SELECT COUNT(fare_amount) AS amount_range FROM AllTrips WHERE fare_amount>100')

task1.select(format_string("0-5,%d",task1.amount_range)).write.save("task2a-sql.out",format="text")
task2.select(format_string("5-15,%d",task2.amount_range)).write.mode("append").save("task2a-sql.out",format="text")   
task3.select(format_string("15-30,%d",task3.amount_range)).write.mode("append").save("task2a-sql.out",format="text")   
task4.select(format_string("30-50,%d",task4.amount_range)).write.mode("append").save("task2a-sql.out",format="text")   
task5.select(format_string("50-100,%d",task5.amount_range)).write.mode("append").save("task2a-sql.out",format="text")   
task6.select(format_string(">100,%d",task6.amount_range)).write.mode("append").save("task2a-sql.out",format="text")   




