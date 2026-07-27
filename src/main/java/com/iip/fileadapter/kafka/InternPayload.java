package com.iip.fileadapter.kafka;

import com.iip.fileadapter.model.InternStatus;

import java.time.LocalDate;

/**
 * The interns contract's payload, as it arrives nested inside a
 * {@link CanonicalEnvelope} (see docs/03-data-model.md §1b). Deliberately
 * not shared with source-service -- the JSON contract is what crosses the
 * boundary, not a Java type.
 *
 * <p>Phase 3.2 renamed this from {@code InternCreatedEvent} and dropped its
 * {@code recordId}/{@code createdAt} fields, which moved up to the envelope.
 * The rename is the point: this is no longer "the event," it is one
 * contract's slice of one.
 */
public record InternPayload(
		String internId,
		String firstName,
		String lastName,
		String email,
		String college,
		String department,
		String mentor,
		LocalDate startDate,
		InternStatus status) {
}
