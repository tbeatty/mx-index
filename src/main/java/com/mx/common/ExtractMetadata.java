package com.mx.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This {@link PTransform} accepts a {@link PCollection} of key and payload {@link KV} and performs
 * the following steps: 1. Extracts the document key. 2. Creates default metadata for each document.
 * 3. For keys where the metadata is found, a {@link PCollection} is returned with {@link KV} of the
 * original payload and the metadata field. 4. For keys where the metadata is not found, a
 * {@link ErrorMessage} object is returned for diagnostic purposes.
 */
@AutoValue
public abstract class ExtractMetadata
        extends PTransform<PCollection<KV<String, String>>, PCollectionTuple> {

    public static Builder newBuilder() {
        return new AutoValue_ExtractMetadata.Builder().setObjectMapper(new ObjectMapper());
    }

    abstract TupleTag<KV<String, String>> successTag();

    abstract TupleTag<ErrorMessage> failureTag();

    abstract ObjectMapper objectMapper();

    @Override
    public PCollectionTuple expand(PCollection<KV<String, String>> input) {
        return input.apply(
                "Extract Metadata",
                ParDo.of(
                        new DoFn<KV<String, String>, KV<String, String>>() {

                            private Storage storage;

                            @Setup
                            public void setup() throws IOException {
                                storage = StorageOptions.getDefaultInstance().getService();
                            }

                            @ProcessElement
                            public void processElement(ProcessContext context) throws IOException {
                                KV<String, String> kv = context.element();
                                String payload = kv.getValue();
                                GcsPath gcsPath = GcsPath.fromUri(kv.getKey());
                                BlobId blobId = BlobId.of(gcsPath.getBucket(), gcsPath.getObject());
                                Blob blob = storage.get(blobId);
                                String metadata = objectMapper().writeValueAsString(blob.getMetadata());
                                context.output(successTag(), KV.of(payload, metadata));
                            }
                        })
                        .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }

    @AutoValue.Builder
    public abstract static class Builder {

        abstract Builder setSuccessTag(TupleTag<KV<String, String>> successTag);

        abstract Builder setFailureTag(TupleTag<ErrorMessage> failureTag);

        abstract Builder setObjectMapper(ObjectMapper objectMapper);

        public abstract ExtractMetadata build();

        public Builder withSuccessTag(TupleTag<KV<String, String>> successTag) {
            checkArgument(successTag != null, "withSuccessTag(successTag) called with null value.");
            return setSuccessTag(successTag);
        }

        public Builder withFailureTag(TupleTag<ErrorMessage> failureTag) {
            checkArgument(failureTag != null, "withFailureTag(failureTag) called with null value.");
            return setFailureTag(failureTag);
        }
    }
}
