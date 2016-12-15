'''
    Sample datasets from update JSON files. 3 datasets per repository.

    Updated on 10/10/2016
    @author: Wei Wei
'''

import json, cPickle, os
from collections import defaultdict
from pprint import pprint

data_dir = "/home/w2wei/biocaddie/data/datamed_json"
sample_dir = "/home/w2wei/biocaddie/data/sample_json"

checklist = defaultdict()
cutoff = 2
doc_num = 0
for doc in os.listdir(data_dir):
    if doc_num%100000==0:
        print "doc num: ",doc_num
    fname = os.path.join(data_dir, doc)
    text = file(fname).read()
    data = json.loads(text)
    # filtering
    repo = data.get('REPOSITORY')
    if not checklist.get(repo):
        checklist[repo]=[doc]
        # checklist[repo]=[data.get('DOCNO')]
    else:
        if len(checklist[repo])<cutoff:
            checklist[repo].append(doc)
            # checklist[repo].append(data.get('DOCNO'))
    doc_num+=1
    
count = 0
for k, v in checklist.iteritems():
    count+=len(v)
print "sample size: ", count
print 

for repo, doc_list in checklist.iteritems():
    for doc in doc_list:
        fin_name = os.path.join(os.path.join(data_dir,doc))
        fin = file(fin_name).read()
        fout_name = os.path.join(os.path.join(sample_dir,doc))
        fout = file(fout_name,"w")
        fout.write(fin)

## examine indexed documents
## current code indexes 20 repositories, 653239 datasets. Some 150000 datasets are missing