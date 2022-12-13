from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# if we want do spark integration with cassandra we need to add sparc-cassandra-connector dependency to add /user/lib/spark/jars forlder.
#https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.11/2.4.2/spark-cassandra-connector_2.11-2.4.2.jar (add proper version)

# reading the table from cassandra
asl = spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option("table",'asl').load()


emp = spark.read.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option("table","emp").load()

# join the emp and asl table.
joindf = asl.join(emp,asl.name==emp.name).drop(emp.name)

#storing the data into cassandra
# before data storing into cassandra we need to define table in cassandra and we need to add https://repo1.maven.org/maven2/com/twitter/jsr166e/1.1.0/jsr166e-1.1.0.jar to /user/lib/spark/jars/    folder.

joindf.write.format("org.apache.spark.sql.cassandra").option('keyspace','cassdb').option('table','join_emp_asl').save()
