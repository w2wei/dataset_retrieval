'''
    Collect primary citaions for datasets from arrayexpress

    Updated on 11/25/2016
    @author Wei Wei
'''
import os,json, urllib2, re, logging, datetime, time,cPickle,sys
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from Bio import Entrez
from geo import analyze_gse_html_for_pmid, analyze_gse_html_for_text
from utils import get_html
Entrez.email = 'granitedewint@gmail.com'

def analyze_landing_html(html, rootURL, logging_html_dir, datasetID, datasetTtl):
    if not html:
        return None
    soup = BeautifulSoup(html,"lxml")
    pat = re.compile("/arrayexpress/experiments/.*-.*-.*/?query=%s"%datasetID)

    urlList = soup.find_all("a", href=pat)

    ttl_pat = re.compile("col_name")
    ttlList = []
    ttlMatches = soup.find_all("td", class_=ttl_pat)
    for ttl in ttlMatches:
        ttlList.append(ttl.get_text())

    destURL = []

    if len(urlList)!=len(ttlList):
        print "mismatched GEO URLs and titles"
        return None
    else:
        size = len(urlList)
        for i in range(size):
            # if urlList[i]==datasetID:
            if ttlList[i]==datasetTtl:
                path = urlList[i]['href']    
                destURL.append(urljoin(rootURL, path))
        if len(destURL)==0:
            print "no URL found"
            return None
        elif len(destURL)>1:
            logging.info("Multiple destination URLs found. ArrayExpress %s"%datasetID)
            print "Multiple URLs found. ArrayExpress %s"%datasetID, destURL
            return None
        else:
            destURL = destURL[0]
            return destURL

def analyze_arrayexpress_html(html, rootURL, logging_html_dir, datasetID):
    if not html:
        return None

    soup = BeautifulSoup(html,"lxml")
    pat = re.compile("http://www.ncbi.nlm.nih.gov/projects/geo")
    urlList = soup.find_all("a", href=pat)

    destURL = []
    for url in urlList:
        path = url["href"]
        destURL.append(urljoin(rootURL, path))
    if len(destURL)==0:
        return None
    elif len(destURL)>1:
        logging.info("Multiple destination URLs found. ArrayExpress %s"%datasetID)
        print "Multiple URLs found: ", destURL
        return None
    else:
        destURL = destURL[0]
        return destURL

def analyze_arrayexpress_json(inFile):
    idList = []
    jsData = json.loads(file(inFile).read())
    dsID = jsData['METADATA']['dataItem']['ID']
    title = jsData['METADATA']['dataItem']['title']
    return [dsID, title]

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "https://www.ebi.ac.uk"
    queryURL = "https://www.ebi.ac.uk/arrayexpress/experiments/?query="

    fname = os.path.basename(inFile)
    dsID, dsTtl = analyze_arrayexpress_json(inFile)
    # print "dsID: ", dsID
    url = "".join([queryURL, dsID])+"&page=1&pagesize=500"
    ## get arrayexpress html
    html = get_html(url, logging_html_dir, dsID, repo)
    ## find link to GEO
    destURL = analyze_landing_html(html, rootURL, logging_html_dir, dsID, dsTtl)
    # print "destURL: ", destURL
    if not destURL:
        return
    destHtml = get_html(destURL, logging_html_dir, dsID, repo)
    geoURL = analyze_arrayexpress_html(destHtml, rootURL, logging_html_dir, dsID)
    # print "geoURL: ", geoURL
    ## get GEO html
    if not geoURL:
        return 
    geoHtml = get_html(geoURL, logging_html_dir, dsID, repo)
    ## get PMID, summary, title, overall design
    pmidList = analyze_gse_html_for_pmid(geoHtml, rootURL, logging_html_dir, dsID)
    summary, title, design = analyze_gse_html_for_text(geoHtml, rootURL, logging_html_dir, dsID)
    ## save summary, title, and overall design
    fsum = file(os.path.join(add_text_dir,fname),"w")
    add_text = ["summary: "+summary, "title: "+title, "design: "+design]    
    fsum.write("\n\n".join(add_text))
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
    repo = "arrayexpress"
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
    print "ArrayExpress skip %d docs\n================="%len(checkList)
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
            print "ArrayExpress doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidList = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)
        fcheck.write(doc+"\n")
        time.sleep(gap)

