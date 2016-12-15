import os,json,time,cPickle
from pprint import pprint

def set_repo_dsID_dict(data_dir, out_file):
    repoList = ["arrayexpress", "bioproject", "clinicaltrials", "dryad", "gemma","geo", "mpd", "neuromorpho", "nursadatasets", "pdb", "proteomexchange"]
    repoDict = {repo:[] for repo in repoList}
    docList = os.listdir(data_dir)
    count=0
    for doc in docList:
        count+=1
        if count%10000==0:
            print 'doc count: ', count
        dat = json.loads(file(os.path.join(data_dir,doc)).read())
        repo = dat['REPOSITORY'].split("_")[0]
        if repoDict.get(repo, None)!=None:
            repoDict[repo].append(doc)
    cPickle.dump(repoDict, file(out_file,"w"))

def split_tasks(inFile, outDir):
    repo_dsID_dict = cPickle.load(file(inFile))
    sub1, sub2, sub3, sub4, sub5, sub6, sub7 = {},{},{},{},{},{},{}
    for repo, docList in repo_dsID_dict.iteritems():
        size = len(docList)/8
        sub1[repo] = docList[:size]
        sub2[repo] = docList[size:size*2]
        sub3[repo] = docList[size*2:size*3]
        sub4[repo] = docList[size*3:size*4]
        sub5[repo] = docList[size*4:size*5]
        sub6[repo] = docList[size*5:size*6]
        sub7[repo] = docList[size*6:]
    cPickle.dump(sub1, file(os.path.join(outDir,"kai.pkl"),"w"))
    cPickle.dump(sub2, file(os.path.join(outDir,"yupeng.pkl"),"w"))
    cPickle.dump(sub3, file(os.path.join(outDir,"sub3.pkl"),"w"))
    cPickle.dump(sub4, file(os.path.join(outDir,"sub4.pkl"),"w"))
    cPickle.dump(sub5, file(os.path.join(outDir,"sub5.pkl"),"w"))
    cPickle.dump(sub6, file(os.path.join(outDir,"sub6.pkl"),"w"))
    cPickle.dump(sub7, file(os.path.join(outDir,"wei.pkl"),"w"))


def sec_split_tasks(inFile, outDir):
    rawFileName = os.path.basename(inFile)[:-4]
    repo_dsID_dict = cPickle.load(file(inFile))
    sub1, sub2, sub3, sub4 = {},{},{},{}
    for repo, docList in repo_dsID_dict.iteritems():
        size = len(docList)/2
        sub1[repo] = docList[:size]
        sub2[repo] = docList[size:]
    cPickle.dump(sub1, file(os.path.join(outDir,"%s_1.pkl"%rawFileName),"w"))
    cPickle.dump(sub2, file(os.path.join(outDir,"%s_2.pkl"%rawFileName),"w"))


if __name__=="__main__":
    data_dir = "/home/w2wei/data2/biocaddie/data/datamed_json"
    repo_dsID_dict_file = "./repo_dsID_dict.pkl"
    sub_repo_dsID_dict_dir = "./sub_repo_dsID_dict"
    if not os.path.exists(sub_repo_dsID_dict_dir):
        os.makedirs(sub_repo_dsID_dict_dir)
    t0=time.time()
    if not os.path.exists(repo_dsID_dict_file):
        set_repo_dsID_dict(data_dir, repo_dsID_dict_file)
    t1=time.time()
    print "time cost: ",t1-t0

    ## first split
    split_tasks(repo_dsID_dict_file, sub_repo_dsID_dict_dir)

    # ## secondary split
    # for doc in os.listdir(sub_repo_dsID_dict_dir):
    #     inFile = os.path.join(sub_repo_dsID_dict_dir,doc)
    #     sec_split_tasks(inFile, sub_repo_dsID_dict_dir)



