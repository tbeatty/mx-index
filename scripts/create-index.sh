#!/usr/bin/env bash

ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST:-localhost:9200}
ELASTICSEARCH_INDEX=${ELASTICSEARCH_INDEX:-mx}

curl -X PUT ${ELASTICSEARCH_HOST}/${ELASTICSEARCH_INDEX} \
  -H 'Content-Type: application/json' \
  -d '
{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "number_of_replicas": 2,
      "analysis": {
        "filter": {
            "stemmer": {
                "type": "stemmer",
                "language": "english"
            },
            "autocompleteFilter": {
                "max_shingle_size": "5",
                "min_shingle_size": "2",
                "type": "shingle"
            },
            "stopwords": {
                "type": "stop",
                "stopwords": ["_english_"]
            }
        },
        "analyzer": {
            "didYouMean": {
                "filter": ["lowercase" ],
                "char_filter": [ "html_strip" ],
                 "type": "custom",
                 "tokenizer": "standard"
            },
            "autocomplete": {
                "filter": ["lowercase", "autocompleteFilter" ],
                "char_filter": ["html_strip"],
                "type": "custom",
                "tokenizer": "standard"
            },
            "default": {
                "filter": ["lowercase","stopwords", "stemmer"],
                "char_filter": ["html_strip"],
                "type": "custom",
                "tokenizer": "standard"
            }
        }
      }
    }
  },
  "mappings": {
    "doc": {
      "properties": {
        "autocomplete": {
           "type": "text",
           "analyzer": "autocomplete",
           "fielddata": true
        },
        "did_you_mean": {
          "type": "text",
          "analyzer": "didYouMean"
        },
        "uri": {
          "type": "keyword"
        },
        "mime_type": {
          "type": "keyword"
        },
        "title": {
          "type": "text"
        },
        "body": {
          "type": "text",
          "copy_to": ["did_you_mean", "autocomplete"]
        },
        "page": {
          "type": "long"
        },
        "language": {
          "type": "nested",
          "properties": {
            "code": {
              "type": "keyword"
            },
            "confidence": {
              "type": "double"
            }
          }
        },
        "filename": {
          "type": "keyword"
        },
        "user_id": {
          "type": "keyword"
        },
        "tag": {
          "type": "keyword"
        },
        "created_date":  {
          "type": "date"
        }
      }
    }
  }
}'
