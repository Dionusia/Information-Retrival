from elasticsearch import Elasticsearch, helpers
import pandas as pd
import json

es = Elasticsearch("http://localhost:9200")
#insert csv to dataframe
df = pd.read_csv('BX-Books.csv')
#convert dataframe to json
j = df.to_json(orient='records')
#json to python dictionary to bulk insert
json_data = json.loads(j)
#delete index if exists
es.indices.delete(index='books', ignore=[400, 404])
#bulk insert of the data
helpers.bulk(es, json_data, index='books')
print("Data successfully loaded!")