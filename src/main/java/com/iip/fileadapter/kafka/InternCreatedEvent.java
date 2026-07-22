package com.iip.fileadapter.kafka;

import com.iip.fileadapter.model.InternStatus;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * The wire shape of an intern.created message (see
 * docs/03-data-model.md §1). Deliberately not shared with source-service --
 * the JSON contract is what crosses the boundary, not a Java type.
 */
public record InternCreatedEvent(
		UUID recordId,
		String internId,
		String firstName,
		String lastName,
		String email,
		String college,
		String department,
		String mentor,
		LocalDate startDate,
		InternStatus status,
		Instant createdAt) {
}
