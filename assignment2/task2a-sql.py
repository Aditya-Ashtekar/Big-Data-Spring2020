import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format
from pandas import *
spark = SparkSession.builder.appName("task2a-sql").config("spark.some.config.option","some-value").getOrCreate()
AllTrips = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])

AllTrips.createOrReplaceTempView("AllTrips")

df = DataFrame({'start':[0,5,15,30,50,100.0], 'end':[5,15,30,50,100,float('inf')]})
df = spark.createDataFrame(df)
df.createOrReplaceTempView("df")
task = spark.sql('SELECT df.start as start, df.end as end, count(AllTrips.fare_amount) as amount FROM df left join AllTrips on AllTrips.fare_amount>df.start and AllTrips.fare_amount<=df.end group by start,end order by start')
task.select(format_string("%f,%f,%d",task.start,task.end,task.amount)).write.save("task2a-sql.out",format="text")

