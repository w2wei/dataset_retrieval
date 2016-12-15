import re
from pprint import pprint


def getIsoEntity(query):
    entityList = query.split("AND")
    isoEntityList = []
    for item in entityList:
        if "AND" in item:
            continue
        elif "OR" in item:
            continue
        elif '\"' in item:
            continue
        else:
            isoEntityList.append(item)
    return isoEntityList

def textAnalyzer(text):
    text = re.findall(("\"(.*)\""),text)
    return text

def clauseAnalyzer(text,result):
    if "OR" in text:
        subText = text.split("OR")
        for sub in subText:
            result.append(clauseAnalyzer(sub,result))
    else:
        if "AND" in text:
            text = text.split("AND")
            text = [textAnalyzer(item) for item in text]
            text = [item for sublist in text for item in sublist]
        else:
            text = textAnalyzer(text)
        return " ".join(text)


if __name__=="__main__":
    # query = """("sequence analysis, protein"[MeSH Terms]) AND bacterial[All Fields] AND ("chemotaxis"[MeSH Terms] OR "chemotaxis"[All Fields])"""
    query = """("sequence analysis, protein"[MeSH Terms] OR ("sequence"[All Fields] AND "analysis"[All Fields] AND "protein"[All Fields]) OR "protein sequence analysis"[All Fields] OR ("protein"[All Fields] AND "sequencing"[All Fields]) OR "protein sequencing"[All Fields] OR "amino acid sequence"[MeSH Terms] OR ("amino"[All Fields] AND "acid"[All Fields] AND "sequence"[All Fields]) OR "amino acid sequence"[All Fields] OR ("protein"[All Fields] AND "sequencing"[All Fields])) AND bacterial[All Fields] AND ("chemotaxis"[MeSH Terms] OR "chemotaxis"[All Fields])"""

    ## clean raw query
    query = re.sub('\[.*?\]','',query)
    print "raw query"
    print query
    raw_input('ck1...')
    isoEntityList = getIsoEntity(query)
    print "isoEntityList"
    pprint(isoEntityList)
    print len(isoEntityList)
    raw_input('ck2...')
    for isoEntity in isoEntityList:
        newIsoEntity = ' ("%s") '%isoEntity.strip()
        query = re.sub(isoEntity, newIsoEntity, query)

    clauseList = query.split(") AND (")
    print "Raw query"
    pprint(clauseList)
    raw_input('ck3...')
    ## extract entities and keep the structure
    result = []
    for clause in clauseList:
        clean_clause=[]
        if "AND" in clause or "OR" in clause:
            clauseAnalyzer(clause,clean_clause)
            result.append(set(clean_clause))
        else:
            clause = textAnalyzer(clause)
            clause = " ".join(clause)
            result.append(set([clause]))

    print "\nAnalyzed query"
    pprint(result)

    ## format json query
