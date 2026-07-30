package com.iip.fileadapter.pipeline;

import com.iip.fileadapter.attachment.Attachment;

/**
 * The step that makes at-least-once delivery safe (Phase 6.2, [Architecture
 * §6](01-architecture.md)).
 *
 * <p>Named as its own interface because §6 calls it mandatory and means it: an
 * adapter that skips this silently breaks the platform's "no duplicate side
 * effects" promise, and the breakage only shows up during a rebalance or a
 * replay, long after the adapter looked finished. An implementer of a new
 * adapter type has to answer this question deliberately rather than discover it.
 *
 * <p>The two existing answers are genuinely different and neither generalizes,
 * which is why this is an interface rather than shared code. A database target
 * gets the guarantee atomically from a constraint; a file or an HTTP endpoint
 * has no such thing and needs a store of what it has already done.
 */
public interface IdempotencyGate {

	/**
	 * Whether this record still needs applying.
	 *
	 * <p>Returning {@code true} for a record already applied is allowed when
	 * the write itself is idempotent -- that is not a weaker guarantee, it is
	 * the same guarantee enforced one layer down, and atomically.
	 */
	boolean shouldApply(RecordEnvelope envelope, Attachment attachment);

	/**
	 * Called after a successful write, for gates that keep their own record of
	 * what has been applied.
	 *
	 * <p>Deliberately after rather than before: a gate that marked a record
	 * seen and then failed to write it would have turned an at-least-once
	 * pipeline into an at-most-once one, silently, for exactly the records that
	 * hit a transient failure.
	 */
	default void markApplied(RecordEnvelope envelope, Attachment attachment) {
	}
}
