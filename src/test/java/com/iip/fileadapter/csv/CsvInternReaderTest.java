package com.iip.fileadapter.csv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.pipeline.RecordEnvelope;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvInternReaderTest {

	@TempDir
	Path tempDir;

	@Test
	void readingAnEmptyOrMissingFileReturnsNoRows() {
		CsvInternReader reader = new CsvInternReader(tempDir.resolve("does-not-exist.csv"));
		assertTrue(reader.readAll().isEmpty());
	}

	/**
	 * Writes through the real, config-driven writer with the same column
	 * mapping the deployment uses. Phase 6.3 moved that mapping out of Java and
	 * into the attachment, and this round trip is part of the evidence it means
	 * the same thing: the reader that has parsed interns.csv since Release 1 is
	 * unchanged and still parses it.
	 */
	private void append(Path csvPath, String json) {
		List<Map<String, String>> columns = List.of(
				Map.of("header", "intern_id", "field", "internId"),
				Map.of("header", "first_name", "field", "firstName"),
				Map.of("header", "last_name", "field", "lastName"),
				Map.of("header", "email", "field", "email"),
				Map.of("header", "college", "field", "college"),
				Map.of("header", "department", "field", "department"),
				Map.of("header", "mentor", "field", "mentor"),
				Map.of("header", "start_date", "field", "startDate"),
				Map.of("header", "status", "field", "status"));

		try {
			new CsvRecordWriter(csvPath).append(
					new RecordEnvelope(new ObjectMapper().readTree(json)),
					new Attachment("test", "interns", Map.of("columns", columns)));
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Test
	void readsBackExactlyWhatTheWriterWrote() {
		Path csvPath = tempDir.resolve("interns.csv");
		UUID recordId = UUID.randomUUID();

		append(csvPath, envelopeJson(recordId.toString(), "INT-READ-1", "Ada", "Lovelace",
				"ada@example.com", "MIT", "Platform Engineering", "Sam"));

		List<InternRow> rows = new CsvInternReader(csvPath).readAll();

		assertEquals(1, rows.size());
		InternRow row = rows.get(0);
		assertEquals(recordId.toString(), row.recordId());
		assertEquals("INT-READ-1", row.internId());
		assertEquals("Ada", row.firstName());
		assertEquals("Lovelace", row.lastName());
		assertEquals("MIT", row.college());
		assertEquals("Sam", row.mentor());
		assertEquals("2026-09-01", row.startDate());
		assertEquals("ACTIVE", row.status());
	}

	@Test
	void correctlyUnquotesAFieldThatContainedACommaWhenWritten() {
		Path csvPath = tempDir.resolve("interns.csv");

		// JSON null rather than a Java null interpolated into the fixture, which
		// would put the four-character string "null" in the payload and test
		// nothing.
		append(csvPath, envelopeJson(UUID.randomUUID().toString(), "INT-READ-2", "Ada", "Lovelace",
				"ada@example.com", "University of X, Y Campus", "Platform Engineering", "Sam")
				.replace("\"mentor\": \"Sam\"", "\"mentor\": null"));

		List<InternRow> rows = new CsvInternReader(csvPath).readAll();

		assertEquals(1, rows.size());
		assertEquals("University of X, Y Campus", rows.get(0).college());
		assertEquals("", rows.get(0).mentor(), "a null mentor round-trips as an empty string, not \"null\"");
	}
}
