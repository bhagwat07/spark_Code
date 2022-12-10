from pyspark.sql import *
from pyspark.sql.functions import *
import configparser
from configparser import *

conf = ConfigParser()
# read the file credential using ConfigParser

cred ="C:\Bigdata\drivers\credential.txt"
conf.read(cred)
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

mshost = conf.get('cred', 'mssqlhost')
mspass = conf.get('cred', 'mssqlpassword')
msuser = conf.get('cred', 'mssqlusername')
msdrive = conf.get('cred', 'mssqldriver')
puser = conf.get("cred", 'pusername')
ppass = conf.get("cred", 'ppassword')
phost = conf.get("cred", 'phost')
pdriver =conf.get("cred",'pdriver')

# reading all table from one(msssql) database and send to the another(postegres) database databases

alltab = spark.read.format('jdbc').option('url', mshost).option('user', msuser).option('password', mspass).option("driver",msdrive)\
        .option("dbtable", "(SELECT table_name FROM INFORMATION_SCHEMA.TABLES  WHERE TABLE_TYPE = 'BASE TABLE')t")\
        .load()
all = [x[0] for x in alltab.collect()]

for x in all:
    df = spark.read.format("jdbc").option('url',mshost).option('user', msuser).option('password', mspass)\
        .option('dbtable',x).option('driver', msdrive).load()
    df.show()
    
    # after reading the data from mssql databases write a all table into postegre database.
    df.write.mode("append").format("jdbc").option("url",phost).option("user", puser).option('password', ppass)\
    .option("dbtable",x).option("driver", pdriver).save()
