package com.iip.fileadapter.csv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses interns.csv back into rows for the admin UI's read/search view --
 * the reverse of CsvInternWriter's RFC4180 quoting (a quoted field can
 * contain a literal comma, and "" inside a quoted field is an escaped
 * quote character, not two separate fields).
 */
public class CsvInternReader {

	private final Path csvPath;

	public CsvInternReader(Path csvPath) {
		this.csvPath = csvPath;
	}

	public List<InternRow> readAll() {
		if (!Files.exists(csvPath)) {
			return List.of();
		}

		List<String> lines;
		try {
			lines = Files.readAllLines(csvPath);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}

		if (lines.size() <= 1) {
			return List.of();
		}

		return lines.stream().skip(1).map(this::parseLine).toList();
	}

	private InternRow parseLine(String line) {
		List<String> fields = parseCsvFields(line);
		return new InternRow(
				fields.get(0), fields.get(1), fields.get(2), fields.get(3), fields.get(4),
				fields.get(5), fields.get(6), fields.get(7), fields.get(8), fields.get(9), fields.get(10));
	}

	private List<String> parseCsvFields(String line) {
		List<String> fields = new ArrayList<>();
		StringBuilder current = new StringBuilder();
		boolean inQuotes = false;

		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if (inQuotes) {
				if (c == '"' && i + 1 < line.length() && line.charAt(i + 1) == '"') {
					current.append('"');
					i++;
				} else if (c == '"') {
					inQuotes = false;
				} else {
					current.append(c);
				}
			} else if (c == '"') {
				inQuotes = true;
			} else if (c == ',') {
				fields.add(current.toString());
				current.setLength(0);
			} else {
				current.append(c);
			}
		}
		fields.add(current.toString());
		return fields;
	}
}
