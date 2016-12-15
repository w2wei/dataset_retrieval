'''
    Format queries and submit them to the IR system

    Updated on 10/26/2016
    @author Wei Wei

    Usage: python main.py gold_standard_dir query.json
'''

from elasticsearch import Elasticsearch, helpers
import json
import cPickle
import time
import sys, os
from pprint import pprint
from load_gstd import read_gold_std
from measures import ndcg_at_k, average_precision
import operator
import numpy as np

# Query Submission parameters
ES_HOST = {"host": "127.0.0.1", "port": 9200}
INDEX_NAME = 'datamed'
es = Elasticsearch(hosts=[ES_HOST])

def search(question_id, query_id, q, out_dir):
    response = es.search(body=q,index = q["indices"]["indices"])
    hit_num = response['hits']['total']

    result = []
    result.append(question_id)
    result.append("Hit num: %d"%hit_num)
    result.append(q)
    print "Question ID: ", question_id
    print "Hit num: ", hit_num
    print "Query: ", q

    for doc in response['hits']['hits']:
        # print pprint(doc)
        result.append(doc)
    
    outFile = os.path.join(out_dir, "%s_%s_ret.raw"%(question_id,query_id))
    fout = file(outFile, "w")
    for item in result:
        if isinstance(item, dict):
            content = json.dumps(item, sort_keys=True, indent=4, separators=(',', ': '))
        else:
            content = item
        fout.write(content+"\n")
        fout.write("="*50+"\n")
    
result_dir = "../../results/G30_expansion/"
if not os.path.exists(result_dir):
    os.makedirs(result_dir)

queries = json.loads(file(sys.argv[1]).read())

for query in queries:
    search(query["id"], query["query_id"], query["query"], result_dir)