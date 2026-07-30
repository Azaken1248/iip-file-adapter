package com.iip.fileadapter.csv;

import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.dedup.DedupStore;
import com.iip.fileadapter.pipeline.IdempotencyGate;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import org.springframework.stereotype.Component;

/**
 * The csv adapter's answer to [Architecture §6](01-architecture.md)'s
 * idempotency gate (Phase 6.2).
 *
 * <p>The opposite of the postgres adapter's, and the contrast is the reason
 * the gate is an interface rather than shared code. "Append a line to a file"
 * is not idempotent and cannot be made so by the target: there is no
 * constraint to lean on, so this adapter has to keep its own record of what it
 * has already written, and that record is the guarantee.
 *
 * <p>{@link #markApplied} is called after the append rather than before, which
 * is what makes redelivery the safe failure. Marking first would mean a record
 * that failed to write was remembered as written -- turning at-least-once into
 * at-most-once for exactly the records that hit a transient error. The other
 * order can duplicate a line if the process dies between the append and the
 * mark; a duplicate is visible and fixable, a silent loss is neither.
 */
@Component
public class DedupStoreIdempotencyGate implements IdempotencyGate {

	private final DedupStore dedupStore;

	public DedupStoreIdempotencyGate(DedupStore dedupStore) {
		this.dedupStore = dedupStore;
	}

	@Override
	public boolean shouldApply(RecordEnvelope envelope, Attachment attachment) {
		return !dedupStore.isProcessed(envelope.recordId());
	}

	@Override
	public void markApplied(RecordEnvelope envelope, Attachment attachment) {
		dedupStore.markProcessed(envelope.recordId());
	}
}
