package com.mx.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import java.io.IOException;
import java.util.Map;

/**
 * A {@link DoFn} that extracts a String value associated with a json payload and adds a new field
 * to the json.
 */
public class InsertMetadataFn extends DoFn<KV<String, String>, String> {

    private ObjectMapper mapper;

    public InsertMetadataFn() {
        this.mapper = new ObjectMapper();
    }

    @ProcessElement
    @SuppressWarnings({"unchecked"})
    public void processElement(ProcessContext context) throws IOException {
        KV<String, String> kv = context.element();
        JsonNode payload = mapper.readTree(kv.getKey());
        JsonNode metadata = mapper.readTree(kv.getValue());
        Map<String, Object> elements = mapper.convertValue(payload, Map.class);
        elements.put("metadata", metadata);
        context.output(mapper.writeValueAsString(mapper.valueToTree(elements)));
    }
}