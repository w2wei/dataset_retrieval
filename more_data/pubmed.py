'''
    Use Entrez to retrieve MEDLINE for given pmids

    Updated 11/23/2016
    @author: Wei Wei
'''
from Bio import Entrez
import os,json, urllib2, re, logging, datetime, sys, cPickle, time
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
Entrez.email = 'granitedewint@gmail.com'


def load_pmids(inFile):
    fin = file(inFile).read().split("\n")
    pmidList = filter(None, fin)
    pmidList = list(set(pmidList))
    return pmidList

def call_eutils(pmidList, outDir):
    size = 100
    iterNum = len(pmidList)/size+1
    for i in range(iterNum):
        pmids = pmidList[i*size:(i+1)*size]
        handle=Entrez.efetch(db='pubmed',id=pmids, rettype="medline", retmode="text")
        data=handle.read()
        handle.close()
        fout = file(os.path.join(outDir, str(i)+"_v3"),"w")
        fout.write(data)
        print "doc count: ", (i+1)*300
    return data

if __name__=="__main__":
    data_dir = "../citation"
    pmid_file = os.path.join(data_dir, "pmids_v3.txt")
    medline_dir = "../medline"
    if not os.path.exists(medline_dir):
        os.makedirs(medline_dir)
    pmidList = load_pmids(pmid_file)

    ## load existing pmids
    ext_pmidList1 = filter(None, file(os.path.join(data_dir, "pmids_v1.txt")).read().split("\n"))
    ext_pmidList2 = filter(None, file(os.path.join(data_dir, "pmids_v2.txt")).read().split("\n"))
    ext_pmidList3 = filter(None, file(os.path.join(data_dir, "pmids_v3.txt")).read().split("\n"))
    ext_pmidList = list(set(ext_pmidList1+ext_pmidList2+ext_pmidList3))
    print "existing pmid num: ", len(ext_pmidList)
    print "all pmid num: ", len(pmidList)
    pmidList = list(set(pmidList)-set(ext_pmidList))
    print "new unique: ", len(pmidList)
    # call_eutils(pmidList, medline_dir)


