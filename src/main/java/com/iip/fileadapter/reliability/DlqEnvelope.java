package com.iip.fileadapter.reliability;

import java.time.Instant;

/**
 * Matches docs/03-data-model.md §3 exactly. Wraps the original payload
 * rather than replacing it, so nothing is lost and the failure is fully
 * diagnosable. replayed/replayedAt are populated by the (Release 7) DLQ
 * Replay Tool -- always false/null when this adapter writes an entry.
 */
public record DlqEnvelope(
		String originalTopic,
		int originalPartition,
		long originalOffset,
		String originalKey,
		String originalPayload,
		String errorType,
		String errorMessage,
		String failedAdapter,
		int attemptCount,
		Instant quarantinedAt,
		boolean replayed,
		Instant replayedAt) {
}
