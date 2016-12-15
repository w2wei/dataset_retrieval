'''
    Collect primary citaions for datasets from clinicaltrials.com

    Updated on 11/23/2016
    @author Wei Wei
'''
import os,json, urllib2, re, logging, datetime, sys, cPickle, time
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from utils import get_html

def analyze_landing_html(html, dsID):
    soup = BeautifulSoup(html,"lxml")
    destLinkList = soup.find_all("a", id="dest_link")
    if len(destLinkList)==0:
        logging.info('No PubMed page found. ClinicalTrials.com, %s'%dsID)
        return None
    elif len(destLinkList)>1:
        logging.info('More than one PubMed page found. ClinicalTrials.com, %s'%dsID)
    pubmedURL = destLinkList[0].get('href')
    return pubmedURL

def extract_pubmed_ID(pubmedURL):
    parsedURL = urlparse(pubmedURL)
    pmid = parsedURL.path.split("/")[-1]
    return pmid

def analyze_clinicaltrials_html(html, rootURL, logging_html_dir, datasetID, repo):
    pmidList = []
    if not html:
        # print "No HTML received"
        return pmidList

    publication_section = html.split("<!-- more_info_section -->")[1].split("<!-- Available Study Data/Document -->")[0]
    try:
        publication_section = re.split('Publications.*:', publication_section)[1]
    except Exception as e:
        # print "No publication section found"
        return pmidList
    soup = BeautifulSoup(publication_section,"lxml")
    studyLinkList = soup.find_all("a", class_="study-link")
    if not studyLinkList:
        # print "No publication found"
        return pmidList
    for link in studyLinkList:
        try:
            linkURL = link.get('onclick')
            if linkURL:
                linkURL = re.findall(".*\(\'(.*).\'\).*", linkURL)
                landingURL = urljoin(rootURL, linkURL[0])
                landingHtml = get_html(landingURL, logging_html_dir, datasetID, repo)
                pubmedURL = analyze_landing_html(landingHtml, datasetID)
                pmid = extract_pubmed_ID(pubmedURL)
                pmidList.append(pmid)
        except Exception as e:
            print e
            logging.info("No publication found. ClinicalTrials.com, %s"%datasetID)
    return list(set(pmidList))

def analyze_clinicaltrials_json(inFile):
    jsData = json.loads(file(inFile).read())
    url = jsData['METADATA']['Study']['homepage']
    dsID = urlparse(url).path.split("/")[-1]
    return [url,dsID]

def run(inFile, repo, logging_html_dir, add_text_dir):
    ## dirs
    rootURL = "https://clinicaltrials.gov"
    ## analysis
    url, dsID = analyze_clinicaltrials_json(inFile)
    html = get_html(url, logging_html_dir, dsID, repo)
    pmidList = analyze_clinicaltrials_html(html, rootURL, logging_html_dir, dsID, repo)
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
    repo = "clinicaltrials"
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
    print "ClinicalTrials skip %d docs\n================="%len(checkList)
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
            print "ClinicalTrials doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidList = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)
        fcheck.write(doc+"\n")
        time.sleep(gap)
    






