'''
    Load gold standard results

    Update on Oct 11, 2016
    @author: Wei Wei
'''

import os, re, cPickle

def get_std():
    try:
        gold_std = cPickle.load(file('../data/std.pkl'))
    except:
        gold_std = {}
        std_dir = "../data/gold_std"
        for doc in os.listdir(std_dir):
            print doc
            raw = file(os.path.join(std_dir,doc))
            text = raw.read()
            text = text.split()
            step=4
            i=0
            group = doc.split('.')[0]
            sub_result = {}
            while i<len(text):
                group, _, docno, label = text[i:i+step]
                i+=step
                sub_result[docno]=int(label)
            gold_std[group]=sub_result
        cPickle.dump(gold_std, file('../data/std.pkl','w'))
    return gold_std

if __name__=='__main__':
    get_std()