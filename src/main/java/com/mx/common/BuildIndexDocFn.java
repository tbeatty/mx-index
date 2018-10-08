package com.mx.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class BuildIndexDocFn extends DoFn<String, String> {

    private ObjectMapper mapper;

    public BuildIndexDocFn() {
        this.mapper = new ObjectMapper();
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        JsonNode payload = mapper.readTree(context.element());
        Map<String, Object> doc = new HashMap<>();
        doc.put("uri", payload.at("/context/uri").asText());
        doc.put("body", payload.at("/fullTextAnnotation/text").asText());
        doc.put("page", payload.at("/context/pageNumber").asInt());
        doc.put("created_date", Instant.now().toString());
        addLanguages(doc, payload);
        addMetadata(doc, payload);
        context.output(mapper.writeValueAsString(doc));
    }

    private void addLanguages(Map<String, Object> doc, JsonNode payload) {
        JsonNode pagesNode = payload.at("/fullTextAnnotation/pages");
        if (!pagesNode.isMissingNode()) {
            JsonNode languagesNode = pagesNode.get(0).at("/property/detectedLanguages");
            if (!languagesNode.isMissingNode()) {
                List<Map<String, Object>> languageDocs = new ArrayList<>();
                languagesNode.elements().forEachRemaining(e ->
                    languageDocs.add(ImmutableMap.of(
                            e.get("languageCode").asText(),
                            e.get("confidence").asDouble()))
                );
                doc.put("language", languageDocs);
            }
        }
    }

    private void addMetadata(Map<String, Object> doc, JsonNode payload) {
        JsonNode userIdNode = payload.at("/metadata/user_id");
        if (!userIdNode.isMissingNode()) {
            doc.put("user_id", userIdNode.asLong());
        }
        JsonNode filenameNode = payload.at("/metadata/filename");
        if (!filenameNode.isMissingNode()) {
            doc.put("filename", filenameNode.asText());
        }
        JsonNode titleNode = payload.at("/metadata/title");
        if (!titleNode.isMissingNode()) {
            doc.put("title", titleNode.asText());
        }
        JsonNode tagsNode = payload.at("/metadata/tags");
        if (!tagsNode.isMissingNode()) {
            doc.put("tag", parseTags(tagsNode.asText()));
        }
    }

    private String[] parseTags(String tags) {
        return Optional.ofNullable(tags)
                .map(s -> s.split(","))
                .orElse(null);
    }
}