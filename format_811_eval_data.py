'''
    Format data for 811 evaluation

    Updated on 11/21/2016
    @author Wei Weis
'''

from elasticsearch import Elasticsearch, helpers
import json
import cPickle
import time
import sys
import numpy as np

# Query Submission parameters
ES_HOST = {"host": "127.0.0.1", "port": 9200}
es = Elasticsearch(hosts=[ES_HOST])

def benchmark(question_id, q):
    query = q.copy()
    del query["indices"]    
    
    response = es.search(body=query,index = q["indices"]["indices"])

    hit_num = response['hits']['total']
    hit_id_list = []

    for doc in response['hits']['hits']:
        hit_id_list.append(doc['_id'])


    fout = file("../../results/recall_oriented/%s.out"%question_id,"w")
    fout.write("\n".join(hit_id_list))


queries = json.loads(file(sys.argv[1]).read())

for query in queries:
    benchmark(query["id"], query["query"])