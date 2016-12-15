'''
	Validate the index

	Updated on Oct 10, 2016
	@author: Wei Wei
'''

from elasticsearch import Elasticsearch
import cPickle, time, os
from pprint import pprint


ES_HOST = {"host" : "127.0.0.1", "port" : 9200}
es = Elasticsearch(hosts = [ES_HOST])

indices=es.indices.get_aliases().keys()
print "Num of indices: ",len(indices)

## sanity check
# res = es.search(body={"from" : 0, "size" : 100,"query": {"match_all": {}}})
# print("sanity check results:")
# print len(res['hits']['hits'])
# for hit in res['hits']['hits']:
#     key = hit['_source'].keys()#['dataset'].keys()#['dataItem'].keys()
#     key.sort()
#     print hit['_id'], key
#     print

## search
response = es.search(body={"from" : 0, "size" : 100, 
    "query": {
        "match": {
            "Study.recruits.criteria": "alcohol"
            # "Dataset.title": "Migraine"
                }
            }
        }
    )
print "response"
print("%d documents found" % response['hits']['total'])
for doc in response['hits']['hits']:
    print("%s) %s" % (doc['_id'], doc['_source']['Dataset']['briefTitle']))


# for tag in response['aggregations']['per_tag']['buckets']:
#     print(tag['key'], tag['max_lines']['value'])