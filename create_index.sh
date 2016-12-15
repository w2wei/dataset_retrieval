# curl -XDELETE '127.0.0.1:9200/test_index?pretty'

curl -XPUT '127.0.0.1:9200/test_index?pretty' -d'
{
    "settings" : {
        "number_of_shards" : 5,
        "number_of_replicas" : 1,
        "analysis" : {
          "filter" : {
            "asciifolding" : {
              "type" : "asciifolding",
              "preserve_original" : "true"
            },
            "english_possessive_stemmer" : {
              "type" : "stemmer",
              "language" : "possessive_english"
            },
            "nlm_stop" : {
              "type" : "stop",
              "stopwords" : [ "a", "about", "again", "all", "almost", "also", "although", "always", "among", "an", "and", "another", "any", "are", "as", "at", "be", "because", "been", "before", "being", "between", "both", "but", "by", "can", "could", "did", "do", "does", "done", "due", "during", "each", "either", "enough", "especially", "etc", "for", "found", "from", "further", "had", "has", "have", "having", "here", "how", "however", "i", "if", "in", "into", "is", "it", "its", "itself", "just", "kg", "km", "made", "mainly", "make", "may", "mg", "might", "ml", "mm", "most", "mostly", "must", "nearly", "neither", "no", "nor", "obtained", "of", "often", "on", "our", "overall", "perhaps", "pmid", "quite", "rather", "really", "regarding", "seem", "seen", "several", "should", "show", "showed", "shown", "shows", "significantly", "since", "so", "some", "such", "than", "that", "the", "their", "theirs", "them", "then", "there", "therefore", "these", "they", "this", "those", "through", "thus", "to", "upon", "use", "used", "using", "various", "very", "was", "we", "were", "what", "when", "which", "while", "with", "within", "without", "would" ]
            },
            "protwords" : {
              "type" : "keyword_marker",
              "keywords_path" : "analysis/mesh_and_entry_vocab.txt"
            },
            "light_english_stemmer" : {
              "type" : "stemmer",
              "language" : "light_english"
            }
          },
          "analyzer" : {
            "default" : {
              "filter" : [ "english_possessive_stemmer", "lowercase", "protwords", "asciifolding", "nlm_stop", "light_english_stemmer" ],
              "tokenizer" : "standard"
            }
          }
        }        
    }
}'


