package com.mx.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface ElasticsearchOptions extends DataflowPipelineOptions {

    @Description("Elasticsearch cluster address(es)")
    @Validation.Required
    String getAddresses();

    void setAddresses(String addresses);

    @Description("Elasticsearch username")
    String getUsername();

    void setUsername(String username);

    @Description("Elasticsearch password")
    String getPassword();

    void setPassword(String password);

    @Description("Name of the Elasticsearch index")
    @Validation.Required
    String getIndex();

    void setIndex(String index);

    @Description("Name of the Elasticsearch type")
    @Validation.Required
    String getType();

    void setType(String type);


}
