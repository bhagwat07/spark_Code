#withColumn used to update existing column if that column already exists.
#lit is one function used to add dummy/default values
#res=df.withColumn("age", lit(18)).withColumn("phone1",regexp_replace(col("phone1"),"-",""))
#above its called dataframe api friendly
#below its called sql friendly (spark.sql)
df.createOrReplaceTempView("tab")
#res=spark.sql("select *, regexp_replace(phone1,'-','') phone from tab ").drop("phone1").withColumnRenamed("phone","phone1")
#res=spark.sql("select first_name, last_name, company_name, address, city, county,state , zip, regexp_replace(phone1,'-','') phone1, phone2, email, web from tab")
res=df.withColumn("state",when(col("state")=="NJ","NewJersey").when(col("state")=="OH","ohio").when(col("state")=="CA","Calif").otherwise(col("state")))\
    .withColumn("email9", when(col("email").contains("cox"),"***").otherwise(col("email")))\
    .withColumn("email0", regexp_replace(col("email"),"aol","***"))\
    .withColumn("fullname",concat(col("first_name"),lit(" "),col("last_name"),lit(" "),col("state")))\
    .withColumn("fname",concat_ws("_",col("first_name"),col("last_name"),col("state")))\
    .withColumn("zip",rpad(col("zip"),5,"0"))\
    .withColumn("phone", concat(lit("***-***-"),substring(col("phone1"),-4,4)))\
    .withColumn("email1", substring(col("email"),0,5))\
    .withColumn("username", substring_index(col("email"),"@",1))\
    .withColumn("mail", substring_index(col("email"),"@",-1))\
    .withColumn("dom",split(col("mail"),"\.")[1])

#res=df.groupBy(col("state")).agg(count("first_name").alias("cnt"),collect_list(col("first_name")).alias("names")).orderBy(col("cnt").desc())
#res=spark.sql("select state, count(*) cnt , collect_list(first_name) allnames from tab group by state order by cnt desc")
#res=spark.sql("select * from tab where zip=(select max(zip) from tab)")
#res=spark.sql("update tab set state='NewYork' where state='ny'")
res1=res.groupBy(col("mail")).agg(count(col("*")).alias("cnt")).orderBy(col("cnt").desc())
res1.show(truncate=False)
