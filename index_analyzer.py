'''
    Follow https://qbox.io/blog/building-an-elasticsearch-index-with-python
    Build a ES index using provided data, using analyzers, stemmers, etc

    Updated on 11/01/2016
    @author Wei Wei
'''
from elasticsearch import Elasticsearch, helpers
import json, cPickle, time, os
from pprint import pprint


ES_HOST = {"host" : "127.0.0.1", "port" : 9200}
# INDEX_NAME = 'datamed'


data_base_dir = "/home/w2wei/biocaddie/data"
data_dir = "/home/w2wei/data2/biocaddie/code/query_benchmark/dataset_samples"
# data_dir = os.path.join(data_base_dir,"datamed_json")

## Load raw data
actions = []
t0=time.time()
count=0

print "sample doc num: ", len(os.listdir(data_dir))
print

for fname in os.listdir(data_dir):
    if count%100000==0:
        print count
    count+=1
    fullpath = os.path.join(data_dir, fname)
    text = file(fullpath).read()#.lower()
    raw_dat = json.loads(text)

    action = {}
    action['_index'] = 'test_index'
    action['_type'] = 'dataset'
    action['_id'] = raw_dat['DOCNO']
    action['_source'] = raw_dat['METADATA']
    action['_score'] = 0
    actions.append(action)
t1=time.time()
print "loading time: ", t1-t0

# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST], chunk_size=1000, timeout=100000)

indices=es.indices.get_aliases().keys()
print "init index num: ",len(indices)

# for idx in indices:
# es.indices.delete(index='test_index', ignore=[400, 404])

## bulk index the data
print("indexing...")
t0=time.time()
helpers.bulk(es, actions)
t1=time.time()
print "indexing time: ", t1-t0

indices=es.indices.get_aliases().keys()    
print "final index num: ",len(indices)
print

indices.sort()
print indices

## sanity check
res = es.search(body={"query": {"match_all": {}}})
print("sanity check results:")
print len(res['hits']['hits'])
for hit in res['hits']['hits']:
    print hit['_id']
print
