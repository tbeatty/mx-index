package com.mx.common;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A class to hold messages that fail validation either because the json is not well formed or
 * because the key element required to be present within the json is missing. The class encapsulates
 * the original payload in addition to a error message and an optional stack trace to help with
 * identifying the root cause in a subsequent reprocessing attempt/debugging.
 */
@AutoValue
public abstract class ErrorMessage {
    public static Builder newBuilder() {
        return new AutoValue_ErrorMessage.Builder();
    }

    public abstract String jsonPayload();

    public abstract String errorMessage();

    @Nullable
    public abstract String errorStackTrace();

    @AutoValue.Builder
    public abstract static class Builder {
        abstract Builder setJsonPayload(String jsonPayload);

        abstract Builder setErrorMessage(String errorMessage);

        abstract Builder setErrorStackTrace(String errorStackTrace);

        public abstract ErrorMessage build();

        public Builder withJsonPayload(String jsonPayload) {
            checkArgument(jsonPayload != null, "withJsonPayload(jsonPayload) called with null value.");
            return setJsonPayload(jsonPayload);
        }

        public Builder withErrorMessage(String errorMessage) {
            checkArgument(errorMessage != null, "withErrorMessage(errorMessage) called with null value.");
            return setErrorMessage(errorMessage);
        }

        public Builder withErrorStackTrace(String errorStackTrace) {
            checkArgument(
                    errorStackTrace != null, "withErrorStackTrace(errorStackTrace) called with null value.");
            return setErrorStackTrace(errorStackTrace);
        }
    }
}
