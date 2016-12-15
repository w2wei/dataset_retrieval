'''
    Read input files, identify the source repository, distribute the task

    Updated on 11/27/2016
    @author Wei Wei
'''
import os,json, urllib2, re, logging, datetime, sys, subprocess, cPickle
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
from Bio import Entrez
from geo import analyze_gse_html_for_pmid, analyze_gse_html_for_text
import arrayexpress, bioproject, clinicaltrials, dryad, gemma, geo, mpd, neuromorpho, nursadatasets, pdb, proteomexchange
Entrez.email = 'granitedewint@gmail.com'


def get_repo(inFile):
    idList = []
    jsData = json.loads(file(inFile).read())
    dsID = jsData['METADATA']['dataItem']['ID']
    return dsID

if __name__=="__main__":
    ## set up dirs
    data_dir = "./data"
    data_dir = "../../data/dataset_samples"
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)    
    log_dir = "./log"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    logging_html_dir = "../logging_html"
    if not os.path.exists(logging_html_dir):
        os.makedirs(logging_html_dir)
    add_text_dir = "../additional_fields"
    if not os.path.exists(add_text_dir):
        os.makedirs(add_text_dir)
    ## set up logging
    repo_list = ["arrayexpress", "bioproject", "clinicaltrials", "dryad", "gemma","geo", "mpd", "neuromorpho", "nursadatasets", "pdb", "proteomexchange"]
    for repo in repo_list:
        logging.basicConfig(filename=os.path.join(log_dir, '%s_%s.log'%(repo, datetime.datetime.now())),level=logging.DEBUG)

    ## identify source repo
    try:
        checkList = filter(None, file("./check_docs.txt").read().split("\n"))
    except:
        checkList = []
    fcheck = file("./check_docs.txt","a")
    docList = os.listdir(data_dir)
    docList = list(set(docList)-set(checkList))
    rec = file("./dataset_pmid_mapping.txt","w")
    docCount=0
    for doc in docList:
        doc+=1
        if docCount%1000==0:
            print "doc count ",docCount

        inFile = os.path.join(data_dir, doc)
        repo = json.loads(file(inFile).read())["REPOSITORY"].split("_")[0]
        pmidList = []
        if repo == 'arrayexpress':
            pmidList = arrayexpress.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'bioproject':
            pmidList = bioproject.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'clinicaltrials':
            pmidList = clinicaltrials.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'dryad':
            pmidList = dryad.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'gemma':
            pmidList = gemma.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'geo':
            pmidList = geo.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'mpd':
            pmidList = mpd.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'neuromorpho':
            pmidList = neuromorpho.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'nursadatasets':
            pmidList = nursadatasets.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'pdb':
            pmidList = pdb.run(inFile, repo, logging_html_dir, add_text_dir)
        if repo == 'proteomexchange':
            pmidList = proteomexchange.run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        checkList.append(doc)
        if pmidList:
            out = "%s : %s\n"%(doc, ";".join(pmidList))
            rec.write(out)








