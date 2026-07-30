package com.iip.fileadapter.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The format this adapter has written since Release 1 (Phase 6.4).
 *
 * <p>Unchanged in every observable way -- the same RFC4180 quoting, the same
 * {@code record_id} first and {@code created_at} last -- because the whole
 * point of extracting the interface was to add formats without disturbing the
 * one that already has readers downstream.
 */
@Component
public class CsvFormatter implements RecordFormatter {

	@Override
	public String id() {
		return "csv";
	}

	@Override
	public Optional<String> header(List<Column> columns) {
		List<String> header = new ArrayList<>();
		header.add("record_id");
		columns.forEach(column -> header.add(column.header()));
		header.add("created_at");
		return Optional.of(String.join(",", header));
	}

	@Override
	public String format(RecordEnvelope envelope, List<Column> columns) {
		JsonNode payload = envelope.payload();
		List<String> values = new ArrayList<>();
		values.add(quote(envelope.recordId().toString()));
		columns.forEach(column -> values.add(quote(Values.text(payload, column.field()))));
		values.add(quote(envelope.occurredAt().toString()));
		return String.join(",", values);
	}

	/**
	 * RFC4180 quoting. Free-text fields could contain a comma or a quote, and a
	 * college name like "University of X, Y Campus" would otherwise silently
	 * corrupt the column count of every row it appeared in.
	 */
	private static String quote(String value) {
		if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
			return "\"" + value.replace("\"", "\"\"") + "\"";
		}
		return value;
	}
}
