package com.iip.fileadapter.csv;

/**
 * A raw row as it actually appears in interns.csv -- deliberately all
 * strings, not parsed back into typed fields, since this is a projection
 * of the file's literal contents (the admin UI's "spreadsheet" view),
 * not a re-derivation of the canonical event.
 */
public record InternRow(
		String recordId,
		String internId,
		String firstName,
		String lastName,
		String email,
		String college,
		String department,
		String mentor,
		String startDate,
		String status,
		String createdAt) {
}
