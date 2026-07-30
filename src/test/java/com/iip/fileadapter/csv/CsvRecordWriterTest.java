package com.iip.fileadapter.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.attachment.AttachmentConfigurationException;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 6.3 -- the file adapter's columns and output file come from the
 * attachment.
 *
 * <p>Was {@code CsvInternWriterTest}, against a writer whose header was a
 * constant and whose body named nine intern fields. The cases it covered are
 * all still here, because none of them were ever about interns: a header
 * written once, RFC4180 quoting, an absent optional field, and appending to an
 * existing file. What has changed is that the columns are supplied rather than
 * compiled in, which is the only reason those cases can now be stated for a
 * contract this repository has never heard of.
 */
class CsvRecordWriterTest {

	@TempDir
	Path tempDir;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static Attachment attachment(String contractId, String path, List<Map<String, String>> columns) {
		Map<String, Object> config = new LinkedHashMap<>();
		if (path != null) {
			config.put("path", path);
		}
		config.put("columns", columns);
		return new Attachment(UUID.randomUUID().toString(), contractId, config);
	}

	/**
	 * A list of {header, field} pairs, not a map. The order is part of the
	 * meaning, and an attachment's config is stored in a jsonb column that does
	 * not preserve an object's key order -- so a map here would be a fixture
	 * shaped unlike anything a real deployment can produce, and this suite would
	 * pass while the deployment wrote rows that did not match their header.
	 */
	private static List<Map<String, String>> internColumns() {
		return List.of(
				Map.of("header", "intern_id", "field", "internId"),
				Map.of("header", "first_name", "field", "firstName"),
				Map.of("header", "college", "field", "college"),
				Map.of("header", "mentor", "field", "mentor"));
	}

	private static RecordEnvelope envelope(String contractId, String payloadJson) {
		try {
			return new RecordEnvelope(MAPPER.readTree("""
					{
					  "recordId": "%s",
					  "contractId": "%s",
					  "recordType": "%s.created",
					  "schemaVersion": 1,
					  "naturalKey": "NK-1",
					  "occurredAt": "2026-07-21T14:10:00Z",
					  "traceId": null,
					  "payload": %s
					}""".formatted(UUID.randomUUID(), contractId, contractId, payloadJson)));
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * <strong>Phase 6.3's completion criterion.</strong> Two contracts, one
	 * adapter instance, two files, two column sets -- and neither knows the
	 * other exists.
	 *
	 * <p>This is what separates a catalog type from a service that happens to
	 * serve one contract. Before this phase the file path and the column list
	 * were deployment configuration, so a second contract meant a second
	 * deployment; the whole of [UC-14](02-use-cases.md) rests on it not
	 * meaning that.
	 */
	@Test
	void twoContractsOnOneInstanceWriteTwoFilesWithTwoColumnSets() throws IOException {
		Path internsFile = tempDir.resolve("interns.csv");
		Path ordersFile = tempDir.resolve("orders.csv");
		CsvRecordWriter writer = new CsvRecordWriter(tempDir.resolve("default.csv"));

		List<Map<String, String>> orderColumns = List.of(
				Map.of("header", "order_number", "field", "orderNumber"),
				Map.of("header", "total", "field", "total"));

		writer.append(
				envelope("interns", "{\"internId\": \"INT-1\", \"firstName\": \"Ada\", "
						+ "\"college\": \"MIT\", \"mentor\": \"Sam\"}"),
				attachment("interns", internsFile.toString(), internColumns()));

		writer.append(
				envelope("purchase-orders", "{\"orderNumber\": \"PO-9\", \"total\": 1200.5}"),
				attachment("purchase-orders", ordersFile.toString(), orderColumns));

		List<String> interns = Files.readAllLines(internsFile);
		List<String> orders = Files.readAllLines(ordersFile);

		assertEquals("record_id,intern_id,first_name,college,mentor,created_at", interns.get(0));
		assertEquals("record_id,order_number,total,created_at", orders.get(0));

		assertEquals(2, interns.size(), "the interns file picked up the other contract's record");
		assertEquals(2, orders.size(), "the orders file picked up the other contract's record");
		assertTrue(interns.get(1).contains("INT-1"));
		assertTrue(orders.get(1).contains("PO-9"));

		// And neither contract's record leaked into the other's file, which a
		// single shared writer holding one path would have done silently.
		assertTrue(!interns.get(1).contains("PO-9"), "an order was written into the interns file");
		assertTrue(!orders.get(1).contains("INT-1"), "an intern was written into the orders file");
	}

	@Test
	void theHeaderIsTheDeclaredColumnsBetweenTheTwoEnvelopeOnes() throws IOException {
		Path file = tempDir.resolve("out.csv");
		new CsvRecordWriter(tempDir.resolve("default.csv")).append(
				envelope("interns", "{\"internId\": \"INT-1\"}"),
				attachment("interns", file.toString(), internColumns()));

		// record_id first and created_at last are the platform's columns, not
		// the contract's, and are deliberately not configurable: record_id is
		// the key the dedup store is built on, and a line that could not be
		// traced back to a record would make the idempotency guarantee
		// unverifiable from the output.
		assertEquals("record_id,intern_id,first_name,college,mentor,created_at",
				Files.readAllLines(file).get(0));
	}

	@Test
	void aFieldContainingACommaIsQuoted() throws IOException {
		Path file = tempDir.resolve("out.csv");
		new CsvRecordWriter(tempDir.resolve("default.csv")).append(
				envelope("interns", "{\"internId\": \"INT-1\", \"college\": \"University of X, Y Campus\"}"),
				attachment("interns", file.toString(), internColumns()));

		String line = Files.readAllLines(file).get(1);
		assertTrue(line.contains("\"University of X, Y Campus\""),
				"an unquoted comma silently corrupts the column count of the row: " + line);
	}

	@Test
	void aFieldTheRecordOmitsIsWrittenEmptyRatherThanAsTheWordNull() throws IOException {
		Path file = tempDir.resolve("out.csv");
		new CsvRecordWriter(tempDir.resolve("default.csv")).append(
				envelope("interns", "{\"internId\": \"INT-1\"}"),
				attachment("interns", file.toString(), internColumns()));

		// A spreadsheet opening this would show the string "null" as a value,
		// which is worse than an empty cell in a way nobody notices until it is
		// in a report.
		assertTrue(Files.readAllLines(file).get(1).endsWith(",,,,2026-07-21T14:10:00Z"),
				"absent fields should be empty: " + Files.readAllLines(file).get(1));
	}

	@Test
	void appendingToAnExistingFileDoesNotRepeatTheHeader() throws IOException {
		Path file = tempDir.resolve("out.csv");
		Attachment attachment = attachment("interns", file.toString(), internColumns());

		// Two writers over one file, which is what a restart looks like: the
		// in-process "have I written a header" flag is gone, and only the file
		// itself can answer.
		new CsvRecordWriter(tempDir.resolve("default.csv"))
				.append(envelope("interns", "{\"internId\": \"INT-1\"}"), attachment);
		new CsvRecordWriter(tempDir.resolve("default.csv"))
				.append(envelope("interns", "{\"internId\": \"INT-2\"}"), attachment);

		List<String> lines = Files.readAllLines(file);
		assertEquals(3, lines.size());
		assertEquals(1, lines.stream().filter(line -> line.startsWith("record_id,")).count(),
				"a restart wrote a second header into the middle of the file");
	}

	@Test
	void anAttachmentWithNoColumnsIsRefusedRatherThanWritingEnvelopesOnly() {
		// Silently writing record_id and created_at and nothing else would
		// produce a file that looks like it works and contains no data.
		assertThrows(AttachmentConfigurationException.class, () ->
				new CsvRecordWriter(tempDir.resolve("default.csv")).append(
						envelope("interns", "{\"internId\": \"INT-1\"}"),
						new Attachment("a", "interns", Map.of())));
	}

	@Test
	void anAttachmentThatNamesNoPathFallsBackToTheDeploymentsFile() throws IOException {
		// What keeps an existing single-contract install writing where it
		// always did, without anyone having to add a path to its attachment.
		Path fallback = tempDir.resolve("default.csv");
		new CsvRecordWriter(fallback).append(
				envelope("interns", "{\"internId\": \"INT-1\"}"),
				new Attachment("a", "interns", Map.of("columns", internColumns())));

		assertTrue(Files.exists(fallback));
		assertTrue(Files.readAllLines(fallback).get(1).contains("INT-1"));
	}
}
