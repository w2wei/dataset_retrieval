'''
    Retrieve bioproject datasets, get pmids of associated citations.
    
    Updated on 11/23/2016
    @author Wei Wei
'''

from Bio import Entrez
import os,json, urllib2, re, logging, datetime, sys, cPickle, time
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
Entrez.email = 'granitedewint@gmail.com'

def analyze_bioproject(inData):
    '''analyze Entrez retrieved bioproject objects and extract pmids'''
    bpid_pmid_dict = {}
    soup = BeautifulSoup(inData,"xml")
    datasetList = soup.find_all("DocumentSummary")
    for dt in datasetList:
        pmidList = []
        try:
            bpid = dt.find_all('ArchiveID')[0]['accession']
            pmidObjs = dt.find_all("Publication")
            for obj in pmidObjs:
                pmidList.append(obj['id'])
            bpid_pmid_dict[bpid] = pmidList
        except Exception as e:
            print e
    return bpid_pmid_dict

def get_bioproject(dsID_list):
    handle=Entrez.efetch(db='bioproject',id=dsID_list)
    data=handle.read()
    handle.close()
    return data

def analyze_bioproject_json(inFile):
    jsData = json.loads(file(inFile).read())
    dsID = jsData['METADATA']['dataItem']['ID']
    return dsID

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "https://www.ncbi.nlm.nih.gov/bioproject/"
    dsID = analyze_bioproject_json(inFile)
    return dsID


if __name__=="__main__":
    ## set up dirs
    data_dir = "../datamed_json"
    # data_dir = sys.argv[1]
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)    
    log_dir = "./log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    logging_html_dir = "./logging_html"
    if not os.path.exists(logging_html_dir):
        os.makedirs(logging_html_dir)
    add_text_dir = "./additional_fields"
    if not os.path.exists(add_text_dir):
        os.makedirs(add_text_dir)
    ## set up logging
    repo = "bioproject"
    logging.basicConfig(filename=os.path.join(log_dir, '%s_%s.log'%(repo, datetime.datetime.now())),level=logging.DEBUG)

    ## identify source repo
    checkFile = os.path.join(log_dir, "./%s_check_docs.txt"%repo)
    try:
        checkList = filter(None, file(checkFile).read().split("\n"))
    except:
        checkList = []
    fcheck = file(checkFile,"w")

    docList = cPickle.load(file('./repo_dsID_dict.pkl'))[repo]
    docList = list(set(docList)-set(checkList))
    result_dir = "./mappings"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    docCount=0
    doc_bpid_dict = {}
    doc_pmid_dict = {}
    t0 = time.time()
    for doc in docList:
        docCount+=1
        inFile = os.path.join(data_dir, doc)
        bpid = run(inFile, repo, logging_html_dir, add_text_dir)
        doc_bpid_dict[doc]=bpid
        ## get pmid for every 1000 doc
        if docCount%100==0:
            print "Bioproject doc count ",docCount
            bpObjs = get_bioproject(doc_bpid_dict.values()) ## retrieve bioproject objects using Entrez.efetch
            bpid_pmid_dict = analyze_bioproject(bpObjs)
            ## merge dicts
            for doc, bpid in doc_bpid_dict.iteritems():
                doc_pmid_dict[doc] = bpid_pmid_dict.get(bpid,[])
            doc_bpid_dict = {} ## reset doc_bpid_dict
    t1 = time.time()
    print "Collecting time: ", t1-t0
    print 
    ## output
    rec = file(os.path.join(result_dir,"./%s_pmid_mapping.txt"%(repo)),"w")
    # out=''    
    for doc, pmidList in doc_pmid_dict.iteritems():
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            try:
                rec.write(out)
            except Exception as e:
                print doc
                print e
                print out 
                print


    print "doc_pmid_dict size: ", len(doc_pmid_dict)
    print "bioproject done"

