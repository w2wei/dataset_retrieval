'''
    Format data for 8-1-1 model. Keep raw text. 
    to format data for 811 model
    1. load raw data
    2. json -> dict
    3. supplement standard fields
    4. connect fields and associated subfields with \.
    5. make each database a separate file; connect entities with \n\n
    6. keep raw text, no pre-processing.
'''

import json, cPickle, time, os, copy, string
from pprint import pprint

# data_base_dir = "/home/w2wei/biocaddie/data"
# data_dir = "/home/w2wei/data2/biocaddie/code/query_benchmark/dataset_samples"
# data_dir = os.path.join(data_base_dir,"datamed_json")
data_dir = "/home/w2wei/biocaddie/data/datamed_json"
formatted_data_dir = "/home/w2wei/biocaddie/data/formatted_datamed_json"
data_files = os.listdir(data_dir)
if not os.path.exists(formatted_data_dir):
    os.makedirs(formatted_data_dir)
## Load raw data
t0=time.time()
count=0

print "sample doc num: ", len(os.listdir(data_dir))

def obj_analysis(inObj, parKeyList, result):
    if isinstance(inObj, dict):
        for key, val in inObj.iteritems():
            localParKeyList = copy.copy(parKeyList)
            localParKeyList.append(key)
            obj_analysis(val, localParKeyList, result)
    elif isinstance(inObj, list):
        for item in inObj:
            localParKeyList = copy.copy(parKeyList)
            localParKeyList.append("")
            obj_analysis(item,localParKeyList,result)
    else:
        try:
            if isinstance(inObj, (int, long, float)):
                inObj = str(inObj)
            text = ''.join([i for i in inObj if ord(i) < 128])
        except Exception as e:
            print e
            print '='*10
            text=''
        result[".".join(parKeyList)]=text
    return result

def write(fname, content):
    fout = file(fname,"w")
    for k, v in content.iteritems():
        line = " : ".join([k,v])+"\n"
        fout.write(line)


for fname in os.listdir(data_dir):
    if count%100000==0:
        print "doc num: ",count
    count+=1
    fullpath = os.path.join(data_dir, fname)
    text = file(fullpath).read()
    raw_dat = json.loads(text)
    result={}
    result = obj_analysis(raw_dat, [],result)
    fname = os.path.join(formatted_data_dir,fname)
    write(fname, result)

t1=time.time()
print "loading time: ", t1-t0


