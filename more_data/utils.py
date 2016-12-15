import os,json, urllib2, re, logging, datetime
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup


def get_html(url, logging_html_dir, dsID, repo):
    try:
        response = urllib2.urlopen(url)
        page = response.read()
        return page
    except Exception as e:
        print e
        # logging_html = "%s.html"%dsID
        # fout = file(os.path.join(logging_html_dir, logging_html),"w")
        # fout.write(url)
        logging.debug("%s %s has no page found"%(repo,dsID))
        return None

def analyze_gse_html(html, rootURL, logging_html_dir, datasetID):
    lines = html.split('\n')
    size = len(lines)
    pmidList, summary, title, design = [],'','',''
    for i in range(size):
        if "<td nowrap>Citation(s)</td>" in lines[i]:
            pmid = lines[i+1]
            pmid = re.findall("pubmed/([0-9]*)\">", pmid)
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
    return [pmidList, summary, title, design]

def analyze_gse_html(html, rootURL, logging_html_dir, datasetID):
    '''extract PMID from GSE using BeautifulSoup'''
    pmidList = []
    if not html:
        return pmidList

    soup = BeautifulSoup(html,"lxml")

    print gseSummary
    print
    ## get PMID 
    pubmedLinkList = soup.find_all("a", href=re.compile("/pubmed/[0-9]+"))
    if not pubmedLinkList:
        return pmidList
    
    for link in pubmedLinkList:
        try:
            linkURL = link.get_text()
            if linkURL.isdigit(): # check if it is pmid
                pmidList.append(linkURL)
        except:
            logging.info("No pmid found. GEO GSE:%s"%datasetID)
    return list(set(pmidList))           

def analyze_dryad_html(html, rootURL, logging_html_dir, datasetID):
    lines = html.split('\n')
    size = len(lines)
    pmidList, summary, title, design = [],'','',''
    for i in range(size):
        if "<td nowrap>Citation(s)</td>" in lines[i]:
            pmid = lines[i+1]
            pmid = re.findall("pubmed/([0-9]*)\">", pmid)
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
    return [pmidList, summary, title, design]     