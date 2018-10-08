package com.mx.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link DoFn} that extracts response objects from a json payload and produces a
 * new json document for each response.
 */
public class SplitResponsesFn extends DoFn<KV<String, String>, KV<String, String>> {

    private ObjectMapper mapper;

    public SplitResponsesFn() {
        this.mapper = new ObjectMapper();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        KV<String, String> kv = context.element();
        String metadata = kv.getValue();
        JsonNode payload = mapper.readTree(kv.getKey());
        JsonNode responses = payload.at("/responses");
        Iterator<JsonNode> iterator = responses.elements();
        while (iterator.hasNext()) {
            String response = mapper.writeValueAsString(mapper.valueToTree(iterator.next()));
            context.output(KV.of(response, metadata));
        }
    }
}