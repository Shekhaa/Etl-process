import http.client

import csv
import json

import pyspark
import pyspark.sql.types
from pyspark.sql.types import *
from pyspark.sql.functions import lit,col,struct,when,cast,sum

conn = http.client.HTTPSConnection("covid-193.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "6eb69c2aa1mshddf27456becc191p108768jsn5185fd3a034a",
    'x-rapidapi-host': "covid-193.p.rapidapi.com"
}

conn.request("GET", "/statistics", headers=headers)

res = conn.getresponse()
data = res.read()
#data is in json format
json_data=json.loads(data.decode('utf-8'))
#data dumped to another json formate where indent is 4
parse_json=json.dumps(json_data,indent=4)
parse=json_data['response']
df=spark.createDataFrame(parse)

#extarcted data to be saved in databrciks
'''df.write.format('json')\
    .option('mode','true')\
    .option('header','True')\
    .option('path','/FileStore/tables/Api_data6/')\
    .save()
'''

dataframe=spark.read.format('json')\
    .option('mode','permissive')\
    .option('inferschema','True')\
    .option('header','True')\
    .load('/FileStore/tables/Api_data6/')

#show method shows only top 20 records

#now transformations is needed
df.printSchema()
#dataframe.select('country','continent','cases.new').show(truncate=False)
df=dataframe\
    .withColumn('cases',struct(when(col('cases.new').isNull(), lit(0)).otherwise(col('cases.new').cast('int')).alias('new')))\
        .withColumn('deaths',struct(when(col('deaths.new').isNull(),lit(0)).otherwise(col('deaths.new').cast('int')).alias('new_deaths')))\
            .withColumn('tests',struct(when(col('tests.total').isNull(),lit(0)).otherwise(col('tests.total').cast('int')).alias('tests_total')))\
                .withColumn('Rate',col('tests.tests_total')/col('population'))

df.printSchema()


cleanedData=df.select(col('Continent'),col('Country'),col('deaths.new_deaths').alias('New_deaths'),col('tests.tests_total').alias('Total_tests'),col('population'),col('Rate'))

cleanedData.show()

print(spark.conf.set('fs.s3a.access.key',your access key'))
print(spark.conf.set('fs.s3a.secret.key',your secret key'))
print(spark.conf.set('fs.s3a.endpoint',"s3.amazonaws.com"))
#we can save this data to s3 or any other platform
s3path='s3a://databrciksdata/PartitiondatawithHeadern'

cleanedData.write.partitionBy('continent').csv(s3path,mode='overwrite',header=True)
#df.select('Continent','country','cases.new').groupBy('Continent').agg(sum('cases.new')).show()
#d=df.withColumn('cases.new',col('cases.new').cast('int'))
#d.printSchema()
#ddf=df.select(col('country'),col('continent'),col('cases.new').cast('int').alias('new'))
#dif=ddf.withColumn('new',col('new').cast('int'))
#df.printSchema()
