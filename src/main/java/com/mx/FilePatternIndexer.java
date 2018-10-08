package com.mx;

import com.mx.coder.ErrorMessageCoder;
import com.mx.common.BuildIndexDocFn;
import com.mx.common.ErrorMessage;
import com.mx.common.ExtractMetadata;
import com.mx.common.InsertMetadataFn;
import com.mx.common.FailSafeValidate;
import com.mx.common.SplitResponsesFn;
import com.mx.options.ElasticsearchOptions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class FilePatternIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(FilePatternIndexer.class);

    /**
     * {@link TupleTag> to tag succesfully validated messages.
     */
    private static final TupleTag<KV<String, String>> VALIDATION_SUCCESS_TAG =
            new TupleTag<KV<String, String>>() {};

    /**
     * {@link TupleTag} to tag messages where metadata is available and extracted.
     */
    private static final TupleTag<KV<String, String>> METADATA_SUCCESS_TAG =
            new TupleTag<KV<String, String>>() {};

    /**
     * {@link TupleTag> to tag failed messages.
     */
    private static final TupleTag<ErrorMessage> FAILURE_TAG = new TupleTag<ErrorMessage>() {};

    /**
     * Options supported by {@link FilePatternIndexer}.
     *
     * <p>Inherits standard configuration options.</p>
     */
    public interface FilePatternIndexerOptions extends PipelineOptions, ElasticsearchOptions {

        @Description("Input pattern")
        @Validation.Required
        String getInputPattern();

        void setInputPattern(String inputPattern);
    }

    /**
     * Runs the pipeline to completion with the specified options.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    private static PipelineResult run(FilePatternIndexerOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(ErrorMessage.class, ErrorMessageCoder.of());

        String[] addresses =
                Lists.newArrayList(Splitter.on(",").trimResults().split(options.getAddresses()))
                        .toArray(new String[0]);

        ElasticsearchIO.ConnectionConfiguration connection =
                ElasticsearchIO.ConnectionConfiguration.create(
                        addresses, options.getIndex(), options.getType());

        PCollection<String> input = pipeline
                .apply("MatchInputPattern", FileIO.match().filepattern(options.getInputPattern()))
                .apply("ReadMatches", FileIO.readMatches().withCompression(Compression.AUTO))
                .apply("ReadFileContents", ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws IOException {
                        FileIO.ReadableFile readableFile = c.element();
                        String content = readableFile.readFullyAsUTF8String();
                        c.output(content);
                    }
                }));

        PCollectionTuple validated = input
                .apply("ValidateDocs",
                        FailSafeValidate.newBuilder()
                            .withSuccessTag(VALIDATION_SUCCESS_TAG)
                            .withFailureTag(FAILURE_TAG)
                            .withKeyPath("/inputConfig/gcsSource/uri")
                            .build());

        PCollectionTuple metadataApplied = validated.get(VALIDATION_SUCCESS_TAG)
                .apply("ExtractMetadata", ExtractMetadata.newBuilder()
                        .withSuccessTag(METADATA_SUCCESS_TAG)
                        .withFailureTag(FAILURE_TAG)
                        .build());

        metadataApplied.get(METADATA_SUCCESS_TAG)
                .apply("SplitResponses", ParDo.of(new SplitResponsesFn()))
                .apply("InsertMetadata", ParDo.of(new InsertMetadataFn()))
                .apply("BuildIndexDoc", ParDo.of(new BuildIndexDocFn()))
                .apply("LogProcessedDoc", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String document = c.element();
                        LOG.info("Json document: {}", document);
                        c.output(document);
                    }
                }))
                .apply("WriteIndex", ElasticsearchIO.write()
                        .withConnectionConfiguration(connection)
                        .withIdFn(doc -> {
                            String val = doc.at("/uri").asText() + "-"
                                    + doc.at("/page").asInt(0);
                            return UUID.nameUUIDFromBytes(val.getBytes()).toString();
                        })
                        .withUsePartialUpdate(true));

        PCollectionList.of(validated.get(FAILURE_TAG))
                .and(metadataApplied.get(FAILURE_TAG))
                .apply("FlattenFailedMessages", Flatten.pCollections())
                .apply("LogFailedMessages", ParDo.of(new DoFn<ErrorMessage, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        LOG.info("Error message: {}", c.element());
                    }
                }));

        return pipeline.run();
    }

    public static void main(String[] args) {
        FilePatternIndexerOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(FilePatternIndexerOptions.class);
        run(options).waitUntilFinish();
    }
}
