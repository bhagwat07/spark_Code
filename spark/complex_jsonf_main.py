# importing a requre packages

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data = "C:\\Bigdata\\drivers\\world_bank.json"

df = spark.read.format("json").option('inferSchema',True).option("mode", 'DROPMALFORMED').load(data)

# write a udf to change complex json datatype to normal datatype............struct,array to string,long

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:     
        # checking the column type is ArrayType 
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
            
        # Similarly checking column type is StructType   
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
                
        else:
            column_list.append(column_name)
            
    # Selecting a columns using column list from DataFrame : df
    
    df = df.select(column_list)
    cols = [re.sub('[^a-zA-Z0-9]', "", c.lower()) for c in df.columns]
    df = df.toDF(*cols)
    return df


def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df)
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True
    return df;


# AND NEXT YOU CAN PROCESS THE DATA

ndf=flatten(df)
ndf.printSchema()
ndf.show(truncate=False)
