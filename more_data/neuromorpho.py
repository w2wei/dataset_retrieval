'''
    Collect primary citaions for datasets from neuromorpho

    Updated on 11/25/2016
    @author Wei Wei
'''
import os,json, urllib2, re, logging, datetime, sys, time, cPickle
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from utils import get_html

def analyze_neuromorpho_html(html, rootURL, logging_html_dir, datasetID):
    pmidList = []
    if not html:
        return pmidList

    content = html.split("\n")
    try:
        for line in content:
            if "PubMed/Abstract Link" in line:
                pmid = re.findall("\'https://www.ncbi.nlm.nih.gov/pubmed/\?term=([0-9]{5,9}})\'\);\"", line)
                pmidList+=pmid
                pmid = re.findall("\'https://www.ncbi.nlm.nih.gov/pubmed/([0-9]{5,9})", line)
                pmidList+=pmid
                pmid = re.findall("\'https://www.ncbi.nlm.nih.gov/.*=([0-9]{5,9})", line)
                pmidList+=pmid
                pmidList = filter(None, pmidList)
    except Exception as e:
        print e
        logging.info("No pmid found. neuromorpho: %s"%datasetID)
    return list(set(pmidList))

def analyze_neuromorpho_json(inFile):
    jsData = json.loads(file(inFile).read())
    url = jsData['METADATA']['dataset']['downloadURL']
    name = jsData['METADATA']['dataset']['title']
    return url, name

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "http://neuromorpho.org/"
    ## analyz json files and retrieve pmids
    neuromorphoURL, dsID = analyze_neuromorpho_json(inFile)
    neuromorphoHtml = get_html(neuromorphoURL, logging_html_dir, dsID, repo)
    pmidList = analyze_neuromorpho_html(neuromorphoHtml, rootURL, logging_html_dir, dsID)
    return pmidList

if __name__=="__main__":
    ## set up dirs
    # data_dir = "/home/w2wei/data2/biocaddie/data/datamed_json"
    data_dir = sys.argv[1]
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
    repo = "neuromorpho"
    logging.basicConfig(filename=os.path.join(log_dir, '%s_%s.log'%(repo, datetime.datetime.now())),level=logging.DEBUG)

    ## identify source repo
    checkFile = os.path.join(log_dir, "./%s_check_docs.txt"%repo)
    try:
        checkList = filter(None, file(checkFile).read().split("\n"))
    except:
        checkList = []
    fcheck = file(checkFile,"w")

    name = sys.argv[2]
    docList = cPickle.load(file('./sub_repo_dsID_dict/%s.pkl'%name))[repo]
    print "Neuromorpho skip %d docs\n================="%len(checkList)
    docList = list(set(docList)-set(checkList))
    result_dir = "./mappings"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    rec = file(os.path.join(result_dir,"./%s_pmid_mapping.txt"%repo),"w")
    docCount=0
    gap = int(sys.argv[3])
    for doc in docList:
        docCount+=1
        if docCount%1000==0:
            print "Neuromorpho doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidList = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)
        fcheck.write(doc+"\n")
        time.sleep(gap)
    
