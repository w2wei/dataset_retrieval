'''
    Pool collected data

    Updated 11/30
    @author: Wei Wei
'''

import os

def pool_pmids(inDir, outFile):
    pmidList = []
    dsidList = []
    for sub in os.listdir(inDir):
        map_dir = os.path.join(inDir, sub, "mappings")
        for doc in os.listdir(map_dir):
            text = filter(None, file(os.path.join(map_dir,doc)).read().split("\n"))
            for rec in text:
                try:
                    dsid, raw = rec.split(" : ")
                    pmids = filter(None, raw.split(";"))
                    pmidList+=pmids
                    dsidList.append(dsid)
                except Exception as e:
                    print e
                    print sub, doc
                    print rec
        try:
            map_dir = os.path.join(inDir, sub, "mappings_akv")
            for doc in os.listdir(map_dir):
                text = filter(None, file(os.path.join(map_dir,doc)).read().split("\n"))
                for rec in text:
                    try:
                        dsid, raw = rec.split(" : ")
                        pmids = filter(None, raw.split(";"))
                        pmidList+=pmids
                        dsidList.append(dsid)
                    except Exception as e:
                        print e
                        print sub, doc
                        print rec 
        except:
            pass         
    pmidList = list(set(pmidList))
    print "PMID num ", len(pmidList)
    print pmidList[:10]
    dsidList = list(set(dsidList))
    print "dsID num: ", len(dsidList)
    print dsidList[:10]
    # fout = file(outFile, "w")
    # fout.write("\n".join(pmidList))




if __name__=="__main__":
    data_dir = "../../data/citation"
    result_dir = "../../results/citation"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    pmid_file = os.path.join(result_dir,"pmids_v4.txt")
    pool_pmids(data_dir, pmid_file)