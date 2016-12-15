'''
    Indexed annotated datasets for E1 - E30. 

    Updated 11/30
    @author: Wei Wei
'''
from elasticsearch import Elasticsearch, helpers
import json, cPickle, time, os, subprocess
from pprint import pprint

def get_docids(inFile):
    text = file(inFile).read().split('\n')
    docList = [line.split("\t")[2]+".json" for line in text]
    docList = list(set(docList))
    return docList

def pool_datasets(inDir, outDir, docList):
    for doc in docList:
        subprocess.call(['cp','%s'%(os.path.join(inDir,doc)),'%s'%(os.path.join(outDir,doc))])


if __name__=="__main__":
    ## get unique docs associated with E1 to E30
    # data_dir = "../data/G30_std"
    # g30_std_file = os.path.join(data_dir, "g30_std.txt")
    g30_std_file = "./g30_std.txt"
    docList = get_docids(g30_std_file)
    print "docList size: ", len(docList)

    ## put all these unique docs in a directory
    datamed_dir = "../../data/datamed_json"
    g30_datamed_dir = "../../data/G30_datamed_json"
    if not os.path.exists(g30_datamed_dir):
        os.makedirs(g30_datamed_dir)
        pool_datasets(datamed_dir, g30_datamed_dir, docList)
    
    ## index these datasets in a dedicated index
    ### First, implement all mapping templates
        '''call bash scripts in ./index_settings'''

    ES_HOST = {"host" : "127.0.0.1", "port" : 9200}

    data_dir = g30_datamed_dir

    ## Load raw data
    actions = []
    t0=time.time()
    count=0
    for fname in os.listdir(data_dir):
        if count%100==0:
            print "doc count: ",count
        count+=1
        fullpath = os.path.join(data_dir, fname)
        text = file(fullpath).read()
        raw_dat = json.loads(text)
        action = {}
        action['_index'] = raw_dat['REPOSITORY'].split("_")[0]+"_std"
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
    #     es.indices.delete(index=idx, ignore=[400, 404])
    # indices=es.indices.get_aliases().keys()    
    # print "ck1: ",indices

    ## bulk index the data
    # print("indexing...")
    # t0=time.time()
    # helpers.bulk(es, actions)
    # t1=time.time()
    # print "indexing time: ", t1-t0

    indices=es.indices.get_aliases().keys()    
    print "final index num: ",len(indices)
    print


    # ## sanity check
    # res = es.search(body={"query": {"match_all": {}}})
    # print("sanity check results:")
    # print len(res['hits']['hits'])
    # for hit in res['hits']['hits']:
    #     print hit['_id']
    # print


