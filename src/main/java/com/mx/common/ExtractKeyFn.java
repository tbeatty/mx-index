package com.mx.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ElasticsearchIO.Write.FieldValueExtractFn} to extract id
 * field from JSON message.
 */
public class ExtractKeyFn implements ElasticsearchIO.Write.FieldValueExtractFn {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractKeyFn.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String jsonKeyPath;

    public ExtractKeyFn(String jsonKeyPath) {
        this.jsonKeyPath = jsonKeyPath;
    }

    @Override
    public String apply(JsonNode input) {
        String fieldValue;
        try {
            JsonNode valueAtPath = input.at(jsonKeyPath);
            if (valueAtPath.isMissingNode()) {
                throw new RuntimeException("Unable to extract id field: " + jsonKeyPath);
            }
            fieldValue = valueAtPath.isTextual()
                    ? valueAtPath.asText()
                    : MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(valueAtPath);
        } catch (JsonProcessingException e) {
            LOG.error("Unable to parse json input: " + input.asText());
            throw new RuntimeException(e);
        }
        return fieldValue;
    }
}
