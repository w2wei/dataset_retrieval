'''
    Retrieve proteomexchange datasets, get pmids of associated citations.
    
    Updated on 11/26/2016
    @author Wei Wei
'''
from Bio import Entrez
import os,json, urllib2, re, logging, datetime, time, sys, cPickle
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from utils import get_html
Entrez.email = 'granitedewint@gmail.com'

def analyze_proteomexchange_html_for_text(html, rootURL, logging_html_dir, datasetID):
    lines = html.split('\n')
    size = len(lines)
    desc=''
    for i in range(size):
        if "<b>Description</b>" in lines[i]:
            desc = re.findall("<b>Description</b>(.*)?<b>", lines[i])
            desc = " ".join(desc)
    return desc    

def analyze_proteomexchange_html_for_pmid(html, rootURL, logging_html_dir, datasetID):
    pmidList = []
    if not html:
        print "no html"
        return pmidList

    soup = BeautifulSoup(html,"lxml")
    ## get PMID 
    # <a href="https://www.ncbi.nlm.nih.gov/pubmed?term=25978296" target="_blank">
    pubmedLinkList = soup.find_all("a", href=re.compile("https://www.ncbi.nlm.nih.gov/pubmed"))
    if not pubmedLinkList:
        return pmidList
    for link in pubmedLinkList:
        try:
            linkURL = link['href']
            pmid = linkURL.split("?term=")[-1]
            if pmid.isdigit(): # check if it is pmid
                pmidList.append(pmid)
        except Exception as e:
            print e
            logging.info("No pmid found. proteomexchange:%s"%datasetID)
    return list(set(pmidList))  


def analyze_proteomexchange_json(inFile):
    jsData = json.loads(file(inFile).read())
    url = jsData['METADATA']['dataset']['downloadURL']
    dsID = jsData['METADATA']['dataset']['ID']
    return [url, dsID]    

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "http://proteomecentral.proteomexchange.org"
    queryURL = "http://proteomecentral.proteomexchange.org/cgi/GetDataset?ID="
    ## analyz json files and retrieve pmids
    fname = os.path.basename(inFile)
    url, dsID = analyze_proteomexchange_json(inFile)
    proteomexchangeHTML = get_html(url, logging_html_dir, dsID, repo)
    pmidList = analyze_proteomexchange_html_for_pmid(proteomexchangeHTML, rootURL, logging_html_dir, dsID)
    desc = analyze_proteomexchange_html_for_text(proteomexchangeHTML, rootURL, logging_html_dir, dsID)
    fout = file(os.path.join(add_text_dir, fname),"w")
    fout.write("description"+desc)
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
    repo = "proteomexchange"
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
    print "proteomexchange skip %d docs\n================="%len(checkList)
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
            print "proteomexchange doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidList = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)
        fcheck.write(doc+"\n")
        time.sleep(gap)

