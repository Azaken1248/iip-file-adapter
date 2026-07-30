package com.iip.fileadapter.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * JSON Lines: one complete JSON object per line (Phase 6.4).
 *
 * <p>Newline-delimited rather than a JSON array, and that is forced by what
 * this adapter is. A single array would have to be re-closed on every append,
 * which means rewriting the tail of the file for each record and gives up the
 * one property that makes appending safe: a partly-written file is still valid
 * up to its last complete line. JSON Lines is also what every log shipper and
 * data warehouse loader already reads.
 *
 * <p>Types survive here in a way they cannot in CSV -- a number stays a number
 * and a boolean stays a boolean -- which is most of the reason to pick this
 * format over the one this adapter started with.
 */
@Component
public class JsonFormatter implements RecordFormatter {

	private final ObjectMapper objectMapper;

	public JsonFormatter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public String id() {
		return "json";
	}

	@Override
	public Optional<String> header(List<Column> columns) {
		// A header line would not be valid JSON, and every reader of a .jsonl
		// file would fail on the first line.
		return Optional.empty();
	}

	@Override
	public String format(RecordEnvelope envelope, List<Column> columns) {
		ObjectNode line = objectMapper.createObjectNode();
		line.put("record_id", envelope.recordId().toString());

		for (Column column : columns) {
			JsonNode value = Values.node(envelope.payload(), column.field());
			// Missing and explicit null both become null: a key that is absent
			// from some lines and present in others is the thing that makes a
			// JSON Lines file annoying to load.
			line.set(column.header(), value.isMissingNode() ? line.nullNode() : value);
		}

		line.put("created_at", envelope.occurredAt().toString());
		return line.toString();
	}
}
