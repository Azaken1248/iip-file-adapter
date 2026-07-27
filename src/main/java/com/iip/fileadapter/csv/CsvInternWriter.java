package com.iip.fileadapter.csv;

import com.iip.fileadapter.kafka.CanonicalEnvelope;
import com.iip.fileadapter.kafka.InternPayload;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Appends CSV lines matching docs/03-data-model.md §4.2's column order.
 * Free-text fields (college, department, mentor) could contain a comma or
 * quote, so fields are RFC4180-quoted rather than naively joined -- a
 * college name like "University of X, Y Campus" would otherwise silently
 * corrupt the column count.
 */
public class CsvInternWriter {

	private static final String HEADER =
			"record_id,intern_id,first_name,last_name,email,college,department,mentor,start_date,status,created_at";

	private final Path outputPath;

	public CsvInternWriter(Path outputPath) {
		this.outputPath = outputPath;
		initializeFileWithHeaderIfAbsent();
	}

	private void initializeFileWithHeaderIfAbsent() {
		try {
			if (outputPath.getParent() != null) {
				Files.createDirectories(outputPath.getParent());
			}
			if (!Files.exists(outputPath)) {
				Files.writeString(outputPath, HEADER + System.lineSeparator());
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	// The CSV column list is unchanged by Phase 3.2's envelope split -- the
	// first and last columns are simply sourced from the envelope now
	// (record_id, and created_at from occurredAt) rather than from a flat
	// event. Downstream consumers of interns.csv see no difference at all,
	// which is the whole intent of the phase.
	public synchronized void append(CanonicalEnvelope envelope) {
		InternPayload payload = envelope.payload();
		String line = String.join(",",
				csvField(envelope.recordId().toString()),
				csvField(payload.internId()),
				csvField(payload.firstName()),
				csvField(payload.lastName()),
				csvField(payload.email()),
				csvField(payload.college()),
				csvField(payload.department()),
				csvField(payload.mentor() == null ? "" : payload.mentor()),
				csvField(payload.startDate().toString()),
				csvField(payload.status().name()),
				csvField(envelope.occurredAt().toString()));

		try {
			Files.writeString(outputPath, line + System.lineSeparator(),
					StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private String csvField(String value) {
		if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
			return "\"" + value.replace("\"", "\"\"") + "\"";
		}
		return value;
	}
}
