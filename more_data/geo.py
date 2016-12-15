'''
    Retrieve GEO datasets, get pmids of associated citations.
    
    Updated on 11/28/2016
    @author Wei Wei
'''
from Bio import Entrez
import os,json, urllib2, re, logging, datetime, time, sys, cPickle
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from utils import get_html
Entrez.email = 'granitedewint@gmail.com'

def analyze_gse_html_for_pmid(html, rootURL, logging_html_dir, datasetID):
    pmidList = []
    if not html:
        # print "empty page"
        return pmidList
    soup = BeautifulSoup(html,"lxml")

    pubmedLinkList = soup.find_all(class_="pubmed_id")
    if not pubmedLinkList:
        # print "no PubMed link found"
        return pmidList

    for link in pubmedLinkList:
        try:
            pmids = link['id']
            pmids = pmids.split(',')
            for pmid in pmids:
                if pmid.isdigit(): # check if it is pmid
                    pmidList.append(pmid.strip())
        except Exception as e:
            print e
            logging.info("No pmid found. GEO GSE:%s"%datasetID)
    return list(set(pmidList))

def analyze_gse_html_for_text(html, rootURL, logging_html_dir, datasetID):
    '''extract PMID from GSE using BeautifulSoup'''
    lines = html.split('\n')
    size = len(lines)
    pmidList, summary, title, design = [],'','',''
    for i in range(size):
        if "<td nowrap>Summary</td>" in lines[i]:
            summary = lines[i+1]
            summary = re.findall("<td style=\"text-align: justify\">(.*)</td>", summary)
            summary = " ".join(summary)
        if "<td nowrap>Title</td>" in lines[i]:
            title = lines[i+1]
            title = re.findall("<td style=\"text-align: justify\">(.*)</td>", title)
            title = " ".join(title)
        if "<td nowrap>Overall design</td>" in lines[i]:
            design = lines[i+1]
            design = re.findall("<td style=\"text-align: justify\">(.*)</td>", design)
            design = " ".join(design)
    return [summary, title, design]     

def analyze_geo_json(inFile):
    jsData = json.loads(file(inFile).read())
    series = jsData['METADATA']['dataItem']['series']
    dsID = jsData['METADATA']['dataItem']['geo_accession']
    return [series, dsID]

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "https://www.ncbi.nlm.nih.gov/geo/"
    queryURL = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc="   
    
    fname = os.path.basename(inFile)
    gseID, gsmID = analyze_geo_json(inFile)
    # call geo.py to get pmid
    seriesURL = "".join([queryURL, gseID])
    ## get GEO html
    seriesHTML = get_html(seriesURL, logging_html_dir, gseID, repo)
    ## get PMID, summary, title, overall design
    pmidList = analyze_gse_html_for_pmid(seriesHTML, rootURL, logging_html_dir, gseID)
    summary, title, design = analyze_gse_html_for_text(seriesHTML, rootURL, logging_html_dir, gseID)
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
    repo = "geo"
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
    print "GEO skip %d docs\n================="%len(checkList)
    docList = list(set(docList)-set(checkList))
    result_dir = "./mappings"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    rec = file(os.path.join(result_dir,"./%s_pmid_mapping.txt"%repo),"w")
    docCount=0
    gap = int(sys.argv[3])
    for doc in docList:
        # print doc
        docCount+=1
        if docCount%1000==0:
            print "GEO doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidList = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)
        fcheck.write(doc+"\n")
        time.sleep(gap)

