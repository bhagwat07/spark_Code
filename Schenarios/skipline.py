dbutils.fs.put('/schenario/skipline.csv','''line1
line2
line3
line3
id,name,loc
1001,amey,mumbai
1002,shantanu,sillod
1003,saurav,pune
1004,yogesh,sambhajinagar''')

# creating a rdd 
rdd = sc.textFile('/schenario/skipline.csv')

''' zip index gives assign the index value
('line1', 0),          0,1,2 assing the value by zipWithIndex().
 ('line2', 1),
 ('line3', 2),
 ('line3', 3),'''

rdd_final = rdd.zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(','))
columns = rdd_final.collect()[0]
skipline = rdd_final.first()
rdd_final.filter(lambda a:a!=skipline).toDF(columns).show()
