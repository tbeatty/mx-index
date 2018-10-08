#!/usr/bin/env bash

ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST:-localhost:9200}
ELASTICSEARCH_INDEX=${ELASTICSEARCH_INDEX:-mx}

curl -X PUT ${ELASTICSEARCH_HOST}/${ELASTICSEARCH_INDEX} -d '
{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "number_of_replicas": 2
    }
  },
  "mappings": {
    "doc": {
      "properties": {
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
          "type": "text"
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
