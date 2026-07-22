package com.iip.fileadapter.csv;

import com.iip.fileadapter.kafka.InternCreatedEvent;
import com.iip.fileadapter.model.InternStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

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

	@Test
	void readsBackExactlyWhatTheWriterWrote() {
		Path csvPath = tempDir.resolve("interns.csv");
		CsvInternWriter writer = new CsvInternWriter(csvPath);

		UUID recordId = UUID.randomUUID();
		writer.append(new InternCreatedEvent(
				recordId, "INT-READ-1", "Ada", "Lovelace", "ada@example.com",
				"MIT", "Platform Engineering", "Sam", LocalDate.of(2026, 9, 1),
				InternStatus.ACTIVE, Instant.parse("2026-07-21T14:10:00Z")));

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
		CsvInternWriter writer = new CsvInternWriter(csvPath);

		writer.append(new InternCreatedEvent(
				UUID.randomUUID(), "INT-READ-2", "Ada", "Lovelace", "ada@example.com",
				"University of X, Y Campus", "Platform Engineering", null, LocalDate.of(2026, 9, 1),
				InternStatus.ACTIVE, Instant.parse("2026-07-21T14:10:00Z")));

		List<InternRow> rows = new CsvInternReader(csvPath).readAll();

		assertEquals(1, rows.size());
		assertEquals("University of X, Y Campus", rows.get(0).college());
		assertEquals("", rows.get(0).mentor(), "a null mentor round-trips as an empty string, not \"null\"");
	}
}
