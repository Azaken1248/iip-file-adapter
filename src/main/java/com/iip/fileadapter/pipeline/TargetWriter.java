package com.iip.fileadapter.pipeline;

import com.iip.fileadapter.attachment.Attachment;

/**
 * The one step that is genuinely this adapter's own (Phase 6.2).
 *
 * <p>Everything else in the pipeline -- the filter, the schema check, the
 * idempotency gate's placement, the classifier, the retry and the DLQ --
 * operates on the envelope and is identical across adapter types. This is where
 * "postgres" or "csv" or "webhook" finally means something, and it is the only
 * place a new adapter type has to think about.
 *
 * <p>The attachment is passed rather than injected because one adapter instance
 * serves every contract attached to it (AD-10): the target table, the file
 * path, the endpoint all come from the attachment, and an implementation that
 * held them as fields would be a bespoke service again.
 */
public interface TargetWriter {

	void write(RecordEnvelope envelope, Attachment attachment);
}
