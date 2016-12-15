'''
    Collect samples of datasets from al repositories

    Updated on Oct 13, 2016
    @author: Wei Wei 
'''
from elasticsearch import Elasticsearch, helpers
import json, cPickle, time, os
from pprint import pprint

data_base_dir = "/home/w2wei/biocaddie/data"
data_dir = os.path.join(data_base_dir,"datamed_json")

## identify 20 repositories
ES_HOST = {"host" : "127.0.0.1", "port" : 9200}
es = Elasticsearch(hosts=[ES_HOST])
indices=es.indices.get_aliases().keys()

## read DATS to understand the schema
for idx in indices:
    response = es.search(body={"query":{"_index":idx}})

    hit_num = response['hits']['total']
    hit_id_list = []

    for doc in response['hits']['hits']:
        hit_id_list.append(doc['_id'])
        
    print "hit num: ", hit_num
    print "hit id list ", len(hit_id_list)
    print

## which fields are "full text" and which are "string" or facet in datamed.org? Find facet fields and re-index datasets