# mx-index

## Running on Dataflow

```
mvn clean package
```

```
java -cp target/mx-index-1.0-SNAPSHOT.jar com.mx.PubSubIndexer  \
  --runner=DataflowRunner \
  --project=steady-thunder-122502 \
  --tempLocation=gs://mx-dataflow/temp \
  --inputSubscription=projects/steady-thunder-122502/subscriptions/mx-index-ingest \
  --rejectionTopic=projects/steady-thunder-122502/topics/mx-index-errors \
  --addresses=https://fe45d38bbe0049eb983e2a6738a315b2.us-central1.gcp.cloud.es.io:9243 \
  --username=elastic \
  --password=<ELASTICSEARCH_PASSWORD> \
  --index=mx \
  --type=doc
```

## Search Highlighting

```
GET /mx/doc/_search?_source_exclude=language,body
{
  "query" : {
    "match": {
      "body": "N226RS"
    }
  },
  "highlight" : {
    "fields" : {
        "body" : {}
    }
  },
  "script_fields" : {
    "thumbnails" : {
      "script" : {
        "lang": "painless",
        "source": "String prefix = /(.*\\/)/.matcher(doc['uri'].value).replaceAll('gs://mx-thumbnails/'); def suffix = doc['page'].value + '.png'; return [prefix + '/100/' + suffix, prefix + '/300/' + suffix, prefix + '/600/' + suffix];"
      }
    }
  }
}
```

## Search with Suggestion

```
GET /mx/_search
{
    "suggest": {
        "did_you_mean": {
            "text": "aircraft",
            "phrase": {
                "field": "did_you_mean"
            }
        }
    },
    "highlight": {
        "fields": {
            "body": {}
        }
    },
    "query": {
        "multi_match": {
            "query": "aircraft",
            "fields": [
                "body"
            ]
        }
    },
    "script_fields": {
        "thumbnails": {
            "script": {
                "lang": "painless",
                "source": "String prefix = /(.*\\/)/.matcher(doc['uri'].value).replaceAll('gs://mx-thumbnails/'); def suffix = doc['page'].value + '.png'; return [prefix + '/100/' + suffix, prefix + '/300/' + suffix, prefix + '/600/' + suffix];"
            }
        }
    }
}
```

## Autocompletion Search

```
GET /mx/doc/_search
{
  "query": {
    "prefix": {
      "autocomplete": "q"
    }
  },
  "aggs": {
    "autocomplete": {
      "terms": {
        "field": "autocomplete",
        "include": "q.*",
        "order": {
          "_count": "desc"
        }
      }
    }
  }
}
```

