'''
    Retrieve pdb datasets, get pmids of associated citations.
    
    Updated on 11/24/2016
    @author Wei Wei
'''
import os, json, logging, datetime, cPickle

def analyze_pdb_json(inFile):
    pmidList = []
    pmidDict = {}
    jsData = json.loads(file(inFile).read())
    try:
        doc = os.path.basename(inFile)
        pmid = jsData['METADATA']['citation']['PMID'].split("pmid:")[1]
        # pmidList.append(pmid)
        pmidDict[doc]=pmid
    except Exception as e:
        fname = os.path.basename(inFile)
        logging.debug("PDB %s has PMID"%fname)
    return pmidDict

def run(inFile, repo, logging_html_dir, add_text_dir):
    rootURL = "http://www.rcsb.org/pdb"
    ## analyz json files and retrieve pmids
    pmidDict = analyze_pdb_json(inFile)
    return pmidDict

if __name__=="__main__":
    ## set up dirs
    data_dir = "/home/w2wei/data2/biocaddie/data/datamed_json"
    # data_dir = sys.argv[1]
    # if not os.path.exists(data_dir):
    #     os.mkdir(data_dir)    
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
    repo = "pdb"
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
    rec = file(os.path.join(result_dir,"./%s_pmid_mapping.txt"%repo),"w")
    docCount=0

    for doc in docList:
        docCount+=1
        if docCount%1000==0:
            print "PDB doc count ",docCount
        inFile = os.path.join(data_dir, doc)
        pmidDict = run(inFile, repo, logging_html_dir, add_text_dir)
        ## output
        if pmidDict.get(doc,[]):
            out = "%s : %s\n"%(doc, pmidDict.get(doc))
            rec.write(out)
        fcheck.write(doc+"\n")