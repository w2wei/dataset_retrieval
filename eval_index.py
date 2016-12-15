'''
    Evaluate the index procedure, including data cleaning, index, etc.

    Updated on 10/11/2016
    @author Wei Wei
'''
from elasticsearch import Elasticsearch, helpers
import json, cPickle, time, os
from pprint import pprint

data_base_dir = "/home/w2wei/biocaddie/data"
data_dir = os.path.join(data_base_dir,"datamed_json")

## Load raw data
actions = []
t0=time.time()
count=0

print "init doc num: ", len(os.listdir(data_dir)) # 794992
print

INDEX = "test_index"

repo_list = []
for fname in os.listdir(data_dir)[4500:4600]:
    count+=1
    fullpath = os.path.join(data_dir, fname)
    text = file(fullpath).read()
    raw_dat = json.loads(text)
    action = {}
    
    action['_index'] = INDEX #raw_dat['REPOSITORY']
    action['_type'] = 'dataset'
    action['_id'] = raw_dat['DOCNO']
    action['_source'] = raw_dat['METADATA']
    action['_score'] = 0
    actions.append(action)
t1=time.time()
print "loading time: ", t1-t0
print "final doc num: ", len(actions)

ES_HOST = {"host" : "127.0.0.1", "port" : 9200}
es = Elasticsearch(hosts=[ES_HOST], chunk_size=1000, timeout=100, stats_only=True)

indices=es.indices.get_aliases().keys()
print "index num: ",len(indices)

es.indices.delete(index=INDEX, ignore=[400, 404])
indices=es.indices.get_aliases().keys()    
print "index num: ",len(indices)
print "index list: ",indices

## bulk index the data
print("indexing...")
t0=time.time()
helpers.bulk(es, actions)
t1=time.time()
print "indexing time: ", t1-t0

# indices=es.indices.get_aliases().keys()    
# print "Index num ",len(indices)


## sanity check
# res = es.search(body={"query": {"match_all": {}}})
# print("sanity check results:")
# print len(res['hits']['hits'])
# for hit in res['hits']['hits']:
#     print hit['_id']
# print
