from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# snowflake credential

sfOptions = {

  "sfUser" : "BHAGWAT",
  "sfURL" : "uflbkmw-hl30212.snowflakecomputing.com",
  "sfpassword":"Bhagwat@07",
  "sfDatabase" : "PRACTICE",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "SF_WH"
}

# read the data(table) from snowflake

df = spark.read.format('net.snowflake.spark.snowflake')\
    .options(**sfOptions) \
    .option('query','select *From EMP')\
    .load()

df.show()

opt = {
    "header" : "true",
    "inferSchema":"true",
    "sep":";"
}

data = "C:\\Bigdata\\drivers\\bank-full.csv"
df2 = spark.read.format('csv').options(**opt).load(data)
df2.show()

#load the data into snowflake.
df2.write.mode("append").format('net.snowflake.spark.snowflake')\
    .options(**sfOptions).option('dbtable','bank').save()
