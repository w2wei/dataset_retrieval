import os, cPickle

# data_dir = "../data/citation"

# vmList = os.listdir(data_dir)
# cit_docid_pmid_dict = {}
# count=0
# for vm in vmList:
# 	sub = os.path.join(data_dir, vm,"mappings")
# 	docList = os.listdir(sub)
# 	for doc in docList:
# 		fin = file(os.path.join(sub, doc))
# 		text = filter(None, fin.read().split("\n"))
# 		for line in text:
# 			try:
# 				docid, pmidList = line.split(" : ")
# 				# print docid, pmidList
# 				pmidList = filter(None, pmidList.split(";"))
# 				cit_docid_pmid_dict[docid]=pmidList
# 			except Exception as e:
# 				print line
# 				print e
# 				count +=1
out_file = "citation_docid_pmid_dict.pkl"
# cPickle.dump(cit_docid_pmid_dict, file(out_file,"w"))
# print "error count: ",count

# print len(cit_docid_pmid_dict)
cit_docid_pmid_dict = cPickle.load(file(out_file))
for k, v in cit_docid_pmid_dict.iteritems():
	print k
	print v
	raw_input('...')