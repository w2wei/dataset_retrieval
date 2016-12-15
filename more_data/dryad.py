'''
    Collect primary citaions for datasets from dryad.org

    Updated on 11/25/2016
    @author Wei Wei
'''
import os,json, urllib2, re, logging, datetime, sys, time, sys, cPickle
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from Bio import Entrez
from utils import get_html
Entrez.email = 'granitedewint@gmail.com'

def analyze_dryad_html(html, rootURL, logging_html_dir, datasetID):
    pmidList = []
    if not html:
        return pmidList
    ## get doi
    soup = BeautifulSoup(html,"lxml")
    citationList = soup.find_all("div", class_="citation-sample")
    doiList = []
    for citation in citationList:
        rawDois = citation.find_all("a", href=re.compile("http://dx.doi.org/"))
        for raw in rawDois:
            doiList.append(raw.get_text())
    doiList = list(set(doiList))
    doiList = [doi.split("http://dx.doi.org/")[-1] for doi in doiList]
    if not doiList:
        return pmidList
    ## get pmids
    for doi in doiList:
        handle=Entrez.esearch(db='pubmed',term=doi+"[Location ID]") ## search PubMed using doi, field type: Location ID, or LID
        data=handle.read()
        handle.close()
        soup = BeautifulSoup(data, "xml")
        idList = soup.find_all('Id')
        pmidList += [pmid.get_text() for pmid in idList]
    return pmidList

def analyze_dryad_json(inFile):
    idList = []
    jsData = json.loads(file(inFile).read())
    identifiers_id = ''
    dataset_dsID = ''
    try:
        idenfifiers_dsIDs = jsData['METADATA']['identifiers']['ID']
        idList = [ds for ds in idenfifiers_dsIDs if ds.startswith('http://hdl.handle.net/')]
        if idList:
            identifiers_id = idList[0]
            identifiers_id = identifiers_id.split("http://hdl.handle.net/")[-1]
    except:
        print "Dryad %s has no identifiers_id"%os.path.basename(inFile)
    try:
        dataset_dsID = jsData['METADATA']['dataset']['ID']
        dataset_dsID = dataset_dsID.split("oai:datadryad.org:")[-1]
    except Exception as e:
        print "Dryad %s has no dataset_ID"%os.path.basename(inFile)
    dsID_list = list(set([identifiers_id, dataset_dsID]))
    if len(dsID_list)==0:
        logging.debug("Dryad %s has no IDs"%os.path.basename(inFile))
        return None
    elif len(dsID_list)==1:
        return dsID_list[0]
    else:
        logging.debug("Dryad %s has multiple IDs"%os.path.basename(inFile))
        return identifiers_id

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "http://datadryad.org/handle/"
    dsID = analyze_dryad_json(inFile)
    if not dsID:
        return []
    url = urljoin(rootURL, dsID)
    dryadHTML = get_html(url, logging_html_dir, dsID, repo)
    pmidList = analyze_dryad_html(dryadHTML, rootURL, logging_html_dir, dsID)
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
    repo = "dryad"
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
    print "Dryad skip %d docs\n================="%len(checkList)
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
            print "Dryad doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidList = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)
        fcheck.write(doc+"\n")
        time.sleep(gap)
