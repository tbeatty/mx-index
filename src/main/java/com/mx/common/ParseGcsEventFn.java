package com.mx.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

public class ParseGcsEventFn extends DoFn<String, String> {

    private ObjectMapper mapper;

    public ParseGcsEventFn() {
        this.mapper = new ObjectMapper();
    }

    @ProcessElement
    @SuppressWarnings({"unchecked"})
    public void processElement(ProcessContext context) throws IOException {
        JsonNode payload = mapper.readTree(context.element());
        String bucket = payload.at("/bucket").asText();
        String name = payload.at("/name").asText();
        String gcsUri = String.format("gs://%s/%s", bucket, name);
        context.output(gcsUri);
    }
}
