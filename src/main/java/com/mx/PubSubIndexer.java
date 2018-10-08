package com.mx;

import com.mx.coder.ErrorMessageCoder;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.mx.common.BuildIndexDocFn;
import com.mx.common.ErrorMessage;
import com.mx.common.ExtractMetadata;
import com.mx.common.FailSafeValidate;
import com.mx.common.InsertMetadataFn;
import com.mx.common.ParseGcsEventFn;
import com.mx.common.SplitResponsesFn;
import com.mx.options.ElasticsearchOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
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

public class PubSubIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubIndexer.class);

    /**
     * {@link TupleTag > to tag succesfully validated messages.
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
    public interface PubSubIndexerOptions extends ElasticsearchOptions, DataflowPipelineOptions {

        @Description("Input PubSub subscription of the form " +
                "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>")
        @Validation.Required
        String getInputSubscription();

        void setInputSubscription(String value);

        @Description(
                "The Cloud Pub/Sub topic to publish rejected messages to. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/topics/<topic-name>.")
        @Validation.Required
        String getRejectionTopic();

        void setRejectionTopic(String rejectionTopic);
    }

    /**
     * Runs the pipeline to completion with the specified options.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    private static PipelineResult run(PubSubIndexerOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry coderRegistry = pipeline.getCoderRegistry();
        coderRegistry.registerCoderForClass(ErrorMessage.class, ErrorMessageCoder.of());

        String[] addresses =
                Lists.newArrayList(Splitter.on(",").trimResults().split(options.getAddresses()))
                        .toArray(new String[0]);

        ElasticsearchIO.ConnectionConfiguration connection =
                ElasticsearchIO.ConnectionConfiguration.create(
                        addresses, options.getIndex(), options.getType()).withUsername("elastic");

        if (options.getUsername() != null) {
            connection = connection
                    .withUsername(options.getUsername())
                    .withPassword(options.getPassword());
        }

        PCollection<String> input = pipeline
                .apply("ReadStream",
                        PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
                .apply("LogMessages", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String message = c.element();
                        LOG.debug("Received message: {}", message);
                        c.output(message);
                    }
                }))
                .apply("ParseGcsEvent", ParDo.of(new ParseGcsEventFn()))
                .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
                .apply(FileIO.readMatches().withCompression(Compression.AUTO))
                .apply("Read files", ParDo.of(new DoFn<FileIO.ReadableFile, String>() {
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
                .apply("WriteFailed",
                        PubsubIO.writeAvros(ErrorMessage.class).to(options.getRejectionTopic()));

        return pipeline.run();
    }

    public static void main(String[] args) {
        PubSubIndexerOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubSubIndexerOptions.class);
        run(options);
    }
}
