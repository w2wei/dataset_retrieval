'''
    Format queries and submit them to the IR system

    Updated on 10/07/2016
    @author Wei Wei

    Usage: python main.py gold_standard_dir query.json
'''

from elasticsearch import Elasticsearch, helpers
import json
import cPickle
import time
import sys
from pprint import pprint
from load_gstd import read_gold_std
from measures import ndcg_at_k, average_precision
import operator
import numpy as np

# Query Submission parameters
ES_HOST = {"host": "127.0.0.1", "port": 9200}
es = Elasticsearch(hosts=[ES_HOST])

def benchmark(question_id, q, std):
    query = q.copy()
    print "selected indices:"
    print q["indices"]["indices"]
    del query["indices"]    
    
    response = es.search(body=query,index = q["indices"]["indices"])

    # response = es.search(body=q)
    hit_num = response['hits']['total']
    hit_id_list = []

    for doc in response['hits']['hits']:
        hit_id_list.append(doc['_id'])

    print "hit num: ", hit_num
    print "hit id list ", len(hit_id_list)
    print len(set(hit_id_list))
    print

    # Compare retrieved results with the gold standard
    gstd = std[question_id]
    rel_score_list = []
    rec_list=[]
    for docno in hit_id_list:
        if docno in rec_list:
            rel_score_list.append(0)
        else:
            rel = gstd.get(docno)
            if rel == 0 or rel == None:
                rel_score_list.append(0)
            else:
                rel_score_list.append(rel)
            rec_list.append(docno)
    # for docno in hit_id_list:
    #     rel = gstd.get(docno)
    #     if rel == 0 or rel == None:
    #         rel_score_list.append(0)
    #     else:
    #         rel_score_list.append(rel)

    print "top 20 rel scores", rel_score_list[:20]
    print "correct # in top %d: "%(len(hit_id_list)),len(filter(None, rel_score_list))
    print 

    # compute NDCG
    gstd = gstd.values()
    print "gstd size: ", len(gstd)
    print gstd
    ndcg = (ndcg_at_k(r=rel_score_list, std=gstd, k=len(hit_id_list), method=1))
    ##ndcg = (ndcg_at_k(r=rel_score_list, std=gstd, k=len(hit_id_list), method=1))

    # compute average precision (AP)
    bin_rel_score_list = [s/s if s>0 else s for s in rel_score_list]
    print "bin_rel_score_list: ", len(bin_rel_score_list), sum(bin_rel_score_list)

    ap = average_precision(bin_rel_score_list)

    # compute precision
    prec = sum(bin_rel_score_list)*1.0/len(bin_rel_score_list)

    # compute recall
    print "gstd"
    print gstd
    print len(gstd)
    gstd_num = len(filter(None, gstd))
    print "gstd_num: ", gstd_num
    gstd_recall = [s/s if s>0 else s for s in gstd]
    recall = sum(bin_rel_score_list)*1.0/gstd_num
    print "recall: ",recall
    # recall = sum(bin_rel_score_list)*1.0/sum(gstd_recall)


    return [ndcg, ap, prec, recall]

gold_standard = read_gold_std(sys.argv[1])
queries = json.loads(file(sys.argv[2]).read())

for query in queries:
    ndcg,ap,prec,recall = benchmark(query["id"], query["query"], gold_standard)
    print "Question: %s, Query_ID: %s\nNDCG: %.3f\nAver Prec: %.3f\nPrec: %.3f\nRecall: %.3f\n"%(query["id"], query["query_id"],ndcg,ap,prec,recall)
