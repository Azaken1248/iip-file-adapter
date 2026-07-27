package com.iip.fileadapter.kafka;

import java.time.Instant;
import java.util.UUID;

/**
 * The wire shape of a canonical envelope (see docs/03-data-model.md §1a).
 * Deliberately not shared with source-service -- the JSON contract is what
 * crosses the boundary, not a Java type.
 *
 * <p>Everything this adapter needs in order to be <em>correct</em> lives on
 * the envelope rather than in the payload: {@code recordId} is the key the
 * dedup store is consulted with, which is what makes an inherently
 * non-idempotent "append a line" operation idempotent in effect. That is why
 * generalizing the platform to many schemas did not require touching the
 * reliability path -- it was already reading only these fields.
 *
 * <p>{@code payload} is typed concretely here because the interns contract
 * is still the only one the adapter writes. Release 6 replaces it with a
 * registry-resolved column mapping over an opaque payload, at which point
 * this type stops naming an intern at all.
 */
public record CanonicalEnvelope(
		UUID recordId,
		String contractId,
		String recordType,
		int schemaVersion,
		String naturalKey,
		Instant occurredAt,
		UUID traceId,
		InternPayload payload) {
}
