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
