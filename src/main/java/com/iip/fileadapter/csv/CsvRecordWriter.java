package com.iip.fileadapter.csv;

import com.fasterxml.jackson.databind.JsonNode;
import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.attachment.AttachmentConfigurationException;
import com.iip.fileadapter.pipeline.RecordEnvelope;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Appends a record to a file, in the columns the attachment declares (Phase
 * 6.3).
 *
 * <p>Replaces {@code CsvInternWriter}, whose header was a constant and whose
 * body named nine intern fields. What that class encoded was "put these payload
 * fields in these columns, in this order" -- a mapping, which is data, and the
 * same mapping the postgres adapter moved into {@code
 * adapter_attachments.config} in Phase 5.7.
 *
 * <p>The two envelope columns are fixed and deliberately not configurable, in
 * the same places they have always been: {@code record_id} first and {@code
 * created_at} last. They are the platform's columns rather than a contract's --
 * {@code record_id} is the key the dedup store is built on, and a file whose
 * lines could not be traced back to a record would make the idempotency
 * guarantee unverifiable from the output. Keeping their positions also means
 * this class writes the existing {@code interns.csv} byte for byte, so nothing
 * downstream of Release 1 notices the change.
 *
 * <p>One instance serves every contract attached to this adapter, which is what
 * makes it a catalog type rather than a bespoke service: the path and the
 * columns arrive with each record, not in a field.
 */
public class CsvRecordWriter {

	private final Path defaultPath;

	/**
	 * Files this instance has already written a header to.
	 *
	 * <p>Not a cache for speed -- it is what stops a second contract's first
	 * record from re-writing a header into the middle of a file that already
	 * has one. The set is per process, and the check below is also guarded by
	 * the file's own existence, so a restart does not duplicate one either.
	 */
	private final Map<Path, Boolean> headerWritten = new ConcurrentHashMap<>();

	public CsvRecordWriter(Path defaultPath) {
		this.defaultPath = defaultPath;
	}

	public synchronized void append(RecordEnvelope envelope, Attachment attachment) {
		List<Column> columns = columnsOf(attachment);
		if (columns.isEmpty()) {
			throw new AttachmentConfigurationException(
					"attachment " + attachment.attachmentId() + " for contract '" + attachment.contractId()
							+ "' declares no columns; expected a list of {header, field} objects, and without "
							+ "them there is nothing to write but the envelope");
		}

		Path path = pathFor(attachment);
		ensureHeader(path, columns);

		JsonNode payload = envelope.payload();
		List<String> values = new ArrayList<>();
		values.add(csvField(envelope.recordId().toString()));
		columns.forEach(column -> values.add(csvField(valueOf(payload, column.field()))));
		values.add(csvField(envelope.occurredAt().toString()));

		write(path, String.join(",", values) + System.lineSeparator());
	}

	/**
	 * A payload field as text, or empty when the contract left it out.
	 *
	 * <p>Empty rather than the string "null", which is what a naive
	 * {@code String.valueOf} would write and what a downstream spreadsheet
	 * would then show as a value. An absent optional field and an empty one are
	 * the same thing in CSV, which has no way to say otherwise.
	 */
	private static String valueOf(JsonNode payload, String field) {
		JsonNode value = payload.path(field);
		if (value.isMissingNode() || value.isNull()) {
			return "";
		}
		return value.isValueNode() ? value.asText() : value.toString();
	}

	private Path pathFor(Attachment attachment) {
		String configured = attachment.configString("path", "");
		return configured.isBlank() ? defaultPath : Path.of(configured);
	}

	/**
	 * The declared columns, in the order they must be written.
	 *
	 * <p>A JSON <em>array</em> of {@code {header, field}} objects rather than
	 * an object keyed by header, because an attachment's config lives in a
	 * {@code jsonb} column and jsonb does not preserve an object's key order --
	 * it sorts keys by length and then bytewise. As an object, this mapping came
	 * back from the registry rearranged, and the adapter wrote rows that did not
	 * match the header it had written above them. A unit test handing a
	 * LinkedHashMap straight to this class cannot see that; the database is the
	 * only thing that can.
	 */
	private static List<Column> columnsOf(Attachment attachment) {
		List<Column> columns = new ArrayList<>();
		for (Map<String, Object> entry : attachment.configList("columns")) {
			Object header = entry.get("header");
			Object field = entry.get("field");
			if (header instanceof String h && field instanceof String f) {
				columns.add(new Column(h, f));
			}
		}
		return columns;
	}

	private record Column(String header, String field) {
	}

	private void ensureHeader(Path path, List<Column> columns) {
		if (headerWritten.containsKey(path)) {
			return;
		}
		try {
			if (path.getParent() != null) {
				Files.createDirectories(path.getParent());
			}
			if (!Files.exists(path)) {
				List<String> header = new ArrayList<>();
				header.add("record_id");
				columns.forEach(column -> header.add(column.header()));
				header.add("created_at");
				Files.writeString(path, String.join(",", header) + System.lineSeparator());
			}
			headerWritten.put(path, true);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void write(Path path, String line) {
		try {
			Files.writeString(path, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * RFC4180 quoting. Free-text fields could contain a comma or a quote, and a
	 * college name like "University of X, Y Campus" would otherwise silently
	 * corrupt the column count of every row it appeared in.
	 */
	private String csvField(String value) {
		if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
			return "\"" + value.replace("\"", "\"\"") + "\"";
		}
		return value;
	}
}
