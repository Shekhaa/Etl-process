import http.client
import csv
import json
conn = http.client.HTTPSConnection("covid-193.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "6eb69c2aa1mshddf27456becc191p108768jsn5185fd3a034a",
    'x-rapidapi-host': "covid-193.p.rapidapi.com"
}

conn.request("GET", "/statistics", headers=headers)
conn.close()
res = conn.getresponse()
data = res.read()
#data is in json format
json_data=json.loads(data.decode('utf-8'))
#data dumped to another json formate where indent is 4
parse_json=json.dumps(json_data,indent=4)
#print(parse_json)
#data is pased for making dataframe
parse=json_data['response']
df=spark.createDataFrame(parse)

#extarcted data to be saved in databrciks
df.write.format('json')\
    .option('mode','true')\
    .option('header','True')\
    .option('path','/FileStore/tables/Api_data1/')\
    .save()

dataframe=spark.read.format('json')\
    .option('mode','permissive')\
    .option('inferschema','True')\
    .option('header','True')\
    .load('/FileStore/tables/Api_data/').show()    


dataframe.select('cases').show()
