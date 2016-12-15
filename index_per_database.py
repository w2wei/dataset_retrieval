'''
    Follow https://qbox.io/blog/building-an-elasticsearch-index-with-python
    Build a ES index using provided data, using analyzers, stemmers, etc
    Index datasets according to the source databases

    Updated on 11/09/2016
    @author Wei Wei
'''
from elasticsearch import Elasticsearch, helpers
import json, cPickle, time, os
from pprint import pprint

VERSION = "1106"
ES_HOST = {"host" : "127.0.0.1", "port" : 9200}
# INDEX_NAME = 'datamed'


data_base_dir = "/home/w2wei/biocaddie/data"
# data_dir = "/home/w2wei/data2/biocaddie/code/query_benchmark/dataset_samples"
data_dir = os.path.join(data_base_dir,"datamed_json")

## Load raw data
actions = []
t0=time.time()
count=0

print "sample doc num: ", len(os.listdir(data_dir))
print

for fname in os.listdir(data_dir):
    if count%100000==0:
        print "doc num: ",count
    count+=1
    fullpath = os.path.join(data_dir, fname)
    text = file(fullpath).read()#.lower()
    raw_dat = json.loads(text)

    action = {}
    action['_id'] = raw_dat['DOCNO']
    action['_score'] = 0
    action['_type'] = 'dataset'
    action['_index'] = "_".join([raw_dat['REPOSITORY'].split("_")[0],VERSION])#'test_index'
    action['_source'] = raw_dat['METADATA']
    if action['_index'].startswith("arrayexpress"):
        try:
            action['_source']['std_description']=action['_source']['dataItem']['description']
        except:
            print "arrayexpress %s does not have a dataItem.description field."%(action['_id'])
        try:
            action['_source']['std_organism']=action['_source']['organism']['experiment']['species']
        except:
            print "arrayexpress %s does not have a organism.experiment.species field."%(action['_id'])
        try:
            action['_source']['std_experimentType']=action['_source']['dataItem']['experimentType']
        except:
            print "arrayexpress %s does not have a dataItem.experimentType field."%(action['_id'])

    if action['_index'].startswith("bioproject"):
        try:
            action['_source']['std_description']=action['_source']['dataItem']['description']
        except:
            print "bioproject %s does not have a dataItem.description field."%(action['_id'])
        organism_names=[]
        try:
            targets=action['_source']['organism']['target'] # targets is a list of organism objects
            for ta in targets:
                try:
                    organism_names.append(ta['species'])
                except:
                    print "bioproject %s does not have a organism.target.species field."%(action['_id'])
                try:
                    organism_names.append(ta['strain'])
                except:
                    print "bioproject %s does not have a organism.target.strain field."%(action['_id'])
        except:
            print "bioproject %s does not have a organism.target field."%(action['_id'])
        action['_source']['std_organism']=organism_names
        try:
            action['_source']['std_keywords']=action['_source']['dataItem']['keywords']
        except:
            action['_source']['std_keywords']=[]
            print "bioproject %s does not have a dataItem.keywords field."%(action['_id'])

    if action['_index'].startswith("cia"):
        try:
            action['_source']['std_organism']=action['_source']['organism']['scientificName']
        except:
            print "cia %s does not have a organism.scientificName field."%(action['_id'])
        action['_source']['std_experimentType']="Imaging"
        try:
            action['_source']['std_anatomicalPart']=[]
            tissue=action['_source']['anatomicalPart']
            for ti in tissue:
                action['_source']['std_anatomicalPart'].append(ti['name'])
        except:
            print "cia %s does not have a anatomicalPart.name field."%(action['_id'])
        try:
            action['_source']['std_disease']=[]
            disease=action['_source']['disease']
            for di in disease:
                action['_source']['std_disease'].append(di['name'])
        except:
            print "cia %s does not have a disease.name field."%(action['_id'])
        try:
            action['_source']['std_datasetStatus']=action['_source']['dataset']['status']
        except:
            print "cia %s does not have a dataset.status field."%(action['_id'])

    if action['_index'].startswith("clinicaltrials"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=". ".join(action['_source']['StudyGroup']['description'])
        except:
            print "clinicaltrails %s does not have StudyGroup.description."%(action['_id'])
        try:
            action['_source']['std_description']+=action['_source']['Study']['recruits']['criteria']
        except:
            print "clinicaltrails %s does not have Study.recruits.criteria."%(action['_id'])
        try:
            action['_source']['std_description']+=action['_source']['Dataset']['description']
        except:
            print "clinicaltrails %s does not have Dataset.description."%(action['_id'])
        action['_source']['std_title']=''
        try:
            action['_source']['std_title']+=action['_source']['Dataset']['title']
        except:
            print "clinicaltrails %s does not have Dataset.title."%(action['_id'])
        try:
            action['_source']['std_title']+=action['_source']['Dataset']['briefTitle']
        except:
            print "clinicaltrails %s does not have Dataset.briefTitle."%(action['_id'])            
        action['_source']['std_experimentType']=['clinical trials']
        try:
            action['_source']['std_experimentType'].append(action['_source']['Study']['studyType'])
        except:
            print "clinicaltrails %s does not have Study.studyType."%(action['_id'])
        try:
            action['_source']['std_gender']=action['_source']['Study']['recruits']['gender']
        except:
            print "clinicaltrails %s does not have Study.recruits.gender."%(action['_id'])
        try:
            action['_source']['std_treatment']=action['_source']['Treatment']['description']
        except:
            print "clinicaltrails %s does not have Treatment.description."%(action['_id'])
        try:
            action['_source']['std_keywords']=action['_source']['Dataset']['keyword']
        except:
            print "clinicaltrails %s does not have Data.keyword."%(action['_id'])
        try:
            action['_source']['std_disease']=action['_source']['Disease']['name']
        except:
            print "clinicaltrails %s does not have Disease.name."%(action['_id'])

    if action['_index'].startswith("ctn"):
        action['_source']['std_organism']=[]
        try:
            organism_list=action['_source']['organism']
            for og in organism_list:
                action['_source']['std_organism'].append(og['scientificName'])
        except:
            print "ctn %s does not have organism.scientificName."%(action['_id'])
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "ctn %s does not have dataset.description."%(action['_id'])
        try:
            action['_source']['std_keywords']=action['_source']['dataset']['keywords']
        except:
            print "ctn %s does not have dataset.keywords."%(action['_id'])

    if action['_index'].startswith("cvrg"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "cvrg %s does not have dataset.description."%(action['_id'])

    if action['_index'].startswith("dataverse"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=action['_source']['dataset']['description']
        except:
            print "dataverse %s does not have dataset.description."%(action['_id'])
        try:
            action['_source']['std_description']+=action['_source']['publication']['description']
        except:
            print "dataverse %s does not have publication.description."%(action['_id'])

    if action['_index'].startswith("dryad"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "dryad %s does not have dataset.description."%(action['_id'])
        try:
            action['_source']['std_keywords']=action['_source']['dataset']['keywords']
        except:
            print "dryad %s does not have dataset.keywords."%(action['_id'])

    if action['_index'].startswith("gemma"):
        try:
            action['_source']['std_description']=action['_source']['dataItem']['description']
        except:
            print "gemma %s does not have dataItem.description."%(action['_id'])

        action['_source']['std_organism']=[]
        try:
            organism_list = action['_source']['organism']['source']
            for og in organism_list:
                action['_source']['std_organism'].append(og['commonName'])
        except:
            print "gemma %s does not have organism.source.commonName"%(action['_id'])

    if action['_index'].startswith("geo"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=(action['_source']['dataItem']['description']+". ")
        except:
            print "geo %s does not have dataItem.description"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['dataItem']['source_name']+". ")
        except:
            print "geo %s does not have dataItem.source_name"%(action['_id'])
        try:
            action['_source']['std_organism']=action['_source']['dataItem']['organism']
        except:
            print "geo %s does not have dataItem.organism"%(action['_id'])

    if action['_index'].startswith("mpd"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=(action['_source']['dataset']['description']+". ")
        except:
            print "mpd %s does not have dataset.description"%(action['_id'])
        try:           
            dimension_list = action['_source']['dimension']
            for dim in dimension_list:
                action['_source']['std_description']+=(dim['name']+". ")
        except:
            print "mpd %s does not have dimension"%(action['_id'])
        try:
            action['_source']['std_experimentType']=action['_source']['dataset']['dataType']
        except:
            print "mpd %s does not have dataset.dataType"%(action['_id'])
        try:
            action['_source']['std_gender']=action['_source']['dataset']['gender']
        except:
            print "mpd %s does not have dataset.gender"%(action['_id'])

        action['_source']['std_organism']=[]
        organism_list=action['_source']['organism']
        for og in organism_list:
            try:
                action['_source']['std_organism'].append(og['scientificName'])
            except:
                print "mpd %s does not have organism.scientificName"%(action['_id'])
            try:
                action['_source']['std_organism'].append(og['strain'])
            except:
                print "mpd %s does not have organism.strain"%(action['_id'])
            try:
                action['_source']['std_organism'].append(og['name'])
            except:
                print "mpd %s does not have organism.name"%(action['_id'])
            
    if action['_index'].startswith("neuromorpho"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['note']
        except:
            print "neuromorpho %s does not have dataset.note"%(action['_id'])
        action['_source']['std_experimentType']='Imaging'
        action['_source']['std_anatomicalPart']=[]
        try:
            action['_source']['std_anatomicalPart']+=(action['_source']['anatomicalPart']['name'])
        except:
            print "neuromorpho %s does not have anatomicalPart.name"%(action['_id'])
        try:
            action['_source']['std_anatomicalPart']+=(action['_source']['cell']['name'])
        except:
            print "neuromorpho %s does not have cell.name"%(action['_id'])
        action['_source']['std_organism']=[]
        try:
            action['_source']['std_organism'].append(action['_source']['organism']['name'])
        except:
            print "neuromorpho %s does not have organism.name"%(action['_id'])
        try:
            action['_source']['std_organism'].append(action['_source']['organism']['strain'])
        except:
            print "neuromorpho %s does not have organism.strain"%(action['_id'])
        try:
            action['_source']['std_gender']=action['_source']['organism']['gender']
        except:
            print "neuromorpho %s does not have organism.gender"%(action['_id'])

    if action['_index'].startswith("nursadatasets"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "nursadatasets %s does not have dataset.description"%(action['_id'])
        try:
            action['_source']['std_keywords']=action['_source']['dataset']['keywords']
        except:
            print "nursadatasets %s does not have dataset.keywords"%(action['_id'])
        try:
            action['_source']['std_experimentType']=action['_source']['dataAcquisition']['title']
        except:
            print "nursadatasets %s does not have dataAcquisition.title"%(action['_id'])

    if action['_index'].startswith("openfmri"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "openfmri %s does not have dataset.description"%(action['_id'])
        try:
            action['_source']['std_experimentType']=action['_source']['dataAcquisition']['title']
        except:
            print "openfmri %s does not have dataAcquisition.title"%(action['_id'])

    if action['_index'].startswith("pdb"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=action['_source']['dataItem']['description']
        except:
            print "pdb %s does not have dataItem.description"%(action['_id'])
        try:
            action['_source']['std_description']+=action['_source']['citation']['title']
        except:
            print "pdb %s does not have citation.title"%(action['_id'])

        action['_source']['std_keywords']=[]
        try:
            action['_source']['std_keywords']+=action['_source']['dataItem']['keywords']
        except:
            print "pdb %s does not have dataItem.keywords"%(action['_id'])

        try:
            gene_list=action['_source']['gene']
            for ge in gene_list:
                action['_source']['std_keywords']+=ge['name']
        except:
            print "pdb %s does not have gene.name"%(action['_id'])

        action['_source']['std_experimentType']='Protein'

        action['_source']['std_organism']=[]
        try:
            host_organism=action['_source']['organism']['host']
            for ho in host_organism:
                try:
                    action['_source']['std_organism'].append(ho['scientificName'])
                except:
                    print host_organism
                    print "pdb %s does not have organism.host.scientificName"%(action['_id'])
                try:
                    action['_source']['std_organism'].append(ho['strain'])
                except:
                    print host_organism
                    print "pdb %s does not have organism.host.stran"%(action['_id'])
        except:
            print "pdb %s does not have organism.host"%(action['_id'])
        try:
            source_organism=action['_source']['organism']['source']
            for so in source_organism:
                try:
                    action['_source']['std_organism'].append(so['scientificName'])
                except:
                    print source_organism
                    print "pdb %s does not have organism.source.scientificName"%(action['_id'])
                try:
                    action['_source']['std_organism'].append(so['strain'])
                except:
                    print source_organism
                    print "pdb %s does not have organism.source.strain"%(action['_id'])
        except:
            print "pdb %s does not have organism.source"%(action['_id'])

    if action['_index'].startswith("peptideatlas"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "peptideatlas %s does not have dataset.description"%(action['_id'])
        action['_source']['std_experimentType']=['Peptide']
        try:
            action['_source']['std_experimentType'].append(action['_source']['instrument']['name'])
        except:
            print "peptideatlas %s does not have instrument.name"%(action['_id'])

    if action['_index'].startswith("phenodisco"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=(action['_source']['topic']+'. ')
        except:
            print "phenodisco %s does not have topic"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['phenName']+'. ')
        except:
            print "phenodisco %s does not have phenName"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['desc']+'. ')
        except:
            print "phenodisco %s does not have desc"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['phen']+'. ')
        except:
            print "phenodisco %s does not have phen"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['phenDesc']+'. ')
        except:
            print "phenodisco %s does not have phenDesc"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['phenMap']+'. ')
        except:
            print "phenodisco %s does not have phenMap"%(action['_id'])
        try:
            action['_source']['std_title']=action['_source']['title']
        except:
            print "phenodisco %s does not have title"%(action['_id'])
        action['_source']['std_keywords']=[]
        try:
            action['_source']['std_keywords'].append(action['_source']['MESHterm'])
        except:
            print "phenodisco %s does not have MESHterm"%(action['_id'])
        try:
            action['_source']['std_keywords'].append(action['_source']['category'])
        except:
            print "phenodisco %s does not have category"%(action['_id'])
        action['_source']['std_experimentType']=[]
        try:
            action['_source']['std_experimentType'].append(action['_source']['Type'])
        except:
            print "phenodisco %s does not have Type"%(action['_id'])
        try:
            action['_source']['std_experimentType'].append(action['_source']['platform'])
        except:
            print "phenodisco %s does not have platform"%(action['_id'])
        try:
            action['_source']['std_experimentType'].append(action['_source']['measurement'])
        except:
            print "phenodisco %s does not have measurement"%(action['_id'])
        try:
            action['_source']['std_disease']=(action['_source']['disease'])
        except:
            print "phenodisco %s does not have disease"%(action['_id'])

    if action['_index'].startswith("physiobank"):
        action['_source']['std_description']=''
        try:
            action['_source']['std_description']+=(action['_source']['dataset']['description']+'. ')
        except:
            print "physiobank %s does not have dataset.description"%(action['_id'])
        try:
            action['_source']['std_description']+=(action['_source']['publication']['name']+'. ')
        except:
            print "physiobank %s does not have publication.name"%(action['_id'])
        try:
            action['_source']['std_experimentType']=action['_source']['dataset']['dataType']
        except:
            print "physiobank %s does not have dataset.dataType"%(action['_id'])

    if action['_index'].startswith("proteomexchange"):
        try:
            action['_source']['std_title']=action['_source']['dataset']['title']
        except:
            print "proteomexchange %s does not have dataset.title"%(action['_id'])
        try:
            action['_source']['std_description']=action['_source']['publication']['name']
        except:
            print "proteomexchange %s does not have publication.name"%(action['_id'])
        try:
            action['_source']['std_experimentType']=action['_source']['instrument']['name']
        except:
            print "proteomexchange %s does not have instrument.name"%(action['_id'])
        try:
            action['_source']['std_keywords']=action['_source']['keywords']
        except:
            print "proteomexchange %s does not have keywords"%(action['_id'])
        
        action['_source']['std_organism']=[]
        try:
            organism_list=action['_source']['organism']
            for og in organism_list:
                action['_source']['std_organism'].append(og['name'])
        except:
            print "proteomexchange %s does not have organism"%(action['_id'])

    if action['_index'].startswith("yped"):
        try:
            action['_source']['std_description']=action['_source']['dataset']['description']
        except:
            print "yped %s does not have dataset.description"%(action['_id'])
        action['_source']['std_experimentType']=['Peptide']
        try:
            action['_source']['std_experimentType'].append(action['_source']['dataAcquisition']['title'])
        except:
            print "yped %s does not have dataAcquisition.title"%(action['_id'])
        try:
            action['_source']['std_organism']=action['_source']['organism']['name']
        except:
            print "yped %s does not have organism.organism"%(action['_id'])

    actions.append(action)

t1=time.time()
print "loading time: ", t1-t0

# # create ES client, create index
es = Elasticsearch(hosts = [ES_HOST], chunk_size=1000, timeout=100000)

# indices=es.indices.get_aliases().keys()
# print "init index num: ",len(indices)

# # for idx in indices:
# # es.indices.delete(index='test_index', ignore=[400, 404])

## bulk index the data
print("indexing...")
t0=time.time()
helpers.bulk(es, actions)
t1=time.time()
print "indexing time: ", t1-t0

# indices=es.indices.get_aliases().keys()    
# print "final index num: ",len(indices)
# print

# indices.sort()
# print indices

# ## sanity check
# res = es.search(body={"query": {"match_all": {}}})
# print("sanity check results:")
# print len(res['hits']['hits'])
# for hit in res['hits']['hits']:
#     print hit['_id']
# print
