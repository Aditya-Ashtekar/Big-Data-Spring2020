import sys
from pyspark.sql import *
from pyspark.sql.functions import format_string,date_format

spark = SparkSession.builder.appName("task1b-sql").config("spark.some.config.option","some-value").getOrCreate()
fares = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
licenses = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

fares.createOrReplaceTempView("fares")
licenses.createOrReplaceTempView("licenses")

Licenses_Fares=  licenses.join(fares,(fares.medallion==licenses.medallion )).select(fares["*"],licenses["*"]).drop(licenses.medallion) 

Licenses_Fares.orderBy('medallion','hack_license','pickup_datetime')
Licenses_Fares.write.option("header","true").csv("task1b-sql.out")



