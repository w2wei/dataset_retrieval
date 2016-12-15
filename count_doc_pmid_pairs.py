import os

data_dir = "../data/citation"
pmidList = []
for subdir in os.listdir(data_dir):
	mapdir = os.path.join(data_dir, subdir, "mappings")
	for doc in os.listdir(mapdir):
		fin = file(os.path.join(mapdir, doc)).read().split('\n')
		for line in fin:
			try:
				pmid = line.split(' : ')[1]
				pmidList.append(pmid)
			except Exception as e:
				print line
print "pmid num: ", len(pmidList)
print len(set(pmidList))