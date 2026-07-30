package com.iip.fileadapter.format;

import com.iip.fileadapter.pipeline.RecordEnvelope;

import java.util.List;
import java.util.Optional;

/**
 * Turns a record into the text of one line in a file (Phase 6.4).
 *
 * <p>The extraction the file adapter needed to stop being the <em>CSV</em>
 * adapter. Everything about delivering a record to a file -- which file, which
 * fields, in what order, written once and only once -- is identical whatever
 * the bytes look like; the only thing that differs is this. Selecting it per
 * attachment means two contracts on one instance can be written in two formats,
 * for the same reason they can already be written to two files.
 *
 * <p>Deliberately not a serializer over the whole envelope. A formatter is
 * given the columns the attachment declared, so which fields reach a file stays
 * the attachment's decision rather than each format having its own opinion --
 * and a contract that adds a field does not silently start appearing in an
 * export nobody updated.
 */
public interface RecordFormatter {

	/**
	 * The value an attachment's {@code format} config selects this by.
	 */
	String id();

	/**
	 * The line to write when the file is created, if this format has one.
	 *
	 * <p>{@link Optional} rather than an empty string because "no header" and
	 * "an empty header line" are different files: JSON Lines and XML fragments
	 * would both be corrupted by a leading blank line, and CSV would gain a
	 * phantom first record.
	 */
	Optional<String> header(List<Column> columns);

	String format(RecordEnvelope envelope, List<Column> columns);
}
