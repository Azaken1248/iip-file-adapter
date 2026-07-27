package com.iip.fileadapter.csv;

import com.iip.fileadapter.kafka.CanonicalEnvelope;
import com.iip.fileadapter.kafka.InternPayload;
import com.iip.fileadapter.model.InternStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvInternWriterTest {

	@TempDir
	Path tempDir;

	// Phase 3.2 moved recordId and the timestamp out of the payload and up
	// into the envelope. Only this fixture changed -- every assertion below
	// is untouched, because the CSV the writer produces is byte-for-byte
	// what it produced before.
	private CanonicalEnvelope event(String internId, String firstName, String college) {
		return envelope(internId, new InternPayload(
				internId, firstName, "Doe", "x@example.com",
				college, "Platform Engineering", "Sam", LocalDate.of(2026, 9, 1),
				InternStatus.ACTIVE));
	}

	private CanonicalEnvelope envelope(String naturalKey, InternPayload payload) {
		return new CanonicalEnvelope(
				UUID.randomUUID(), "interns", "intern.created", 1, naturalKey,
				Instant.parse("2026-07-21T14:10:00Z"), null, payload);
	}

	@Test
	void writingTwoDifferentRecordsProducesTwoDistinctLines() throws IOException {
		Path csvPath = tempDir.resolve("interns.csv");
		CsvInternWriter writer = new CsvInternWriter(csvPath);

		writer.append(event("INT-CSV-1", "Ada", "MIT"));
		writer.append(event("INT-CSV-2", "Grace", "Yale"));

		List<String> lines = Files.readAllLines(csvPath);
		assertEquals(3, lines.size(), "expected a header line plus two data lines");
		assertEquals(
				"record_id,intern_id,first_name,last_name,email,college,department,mentor,start_date,status,created_at",
				lines.get(0));
		assertTrue(lines.get(1).contains("INT-CSV-1"));
		assertTrue(lines.get(1).contains("Ada"));
		assertTrue(lines.get(2).contains("INT-CSV-2"));
		assertTrue(lines.get(2).contains("Grace"));
		assertTrue(lines.get(1).endsWith(",ACTIVE,2026-07-21T14:10:00Z"));
	}

	@Test
	void aFieldContainingACommaIsQuoted() throws IOException {
		Path csvPath = tempDir.resolve("interns.csv");
		CsvInternWriter writer = new CsvInternWriter(csvPath);

		writer.append(event("INT-CSV-3", "Ada", "University of X, Y Campus"));

		List<String> lines = Files.readAllLines(csvPath);
		assertTrue(lines.get(1).contains("\"University of X, Y Campus\""),
				"expected the comma-containing field to be quoted, got: " + lines.get(1));
	}

	@Test
	void aNullMentorWritesAnEmptyField() throws IOException {
		Path csvPath = tempDir.resolve("interns.csv");
		CsvInternWriter writer = new CsvInternWriter(csvPath);

		CanonicalEnvelope noMentor = envelope("INT-CSV-4", new InternPayload(
				"INT-CSV-4", "Ada", "Doe", "x@example.com",
				"MIT", "Platform Engineering", null, LocalDate.of(2026, 9, 1),
				InternStatus.ACTIVE));

		writer.append(noMentor);

		List<String> lines = Files.readAllLines(csvPath);
		assertEquals("INT-CSV-4,Ada,Doe,x@example.com,MIT,Platform Engineering,,2026-09-01,ACTIVE,2026-07-21T14:10:00Z",
				lines.get(1).substring(lines.get(1).indexOf(',') + 1));
	}

	@Test
	void reopeningAnExistingFileDoesNotDuplicateTheHeader() throws IOException {
		Path csvPath = tempDir.resolve("interns.csv");
		new CsvInternWriter(csvPath).append(event("INT-CSV-5", "Ada", "MIT"));
		new CsvInternWriter(csvPath).append(event("INT-CSV-6", "Grace", "Yale"));

		List<String> lines = Files.readAllLines(csvPath);
		assertEquals(3, lines.size(), "expected exactly one header line across two writer instances");
	}
}
