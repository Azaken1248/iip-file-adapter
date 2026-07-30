package com.iip.fileadapter.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.attachment.AttachmentConfigurationException;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 6.4 -- the file adapter can write more than CSV, chosen per attachment.
 *
 * <p>What the extraction is worth: everything about delivering a record to a
 * file -- which file, which fields, in what order, written once and only once --
 * is identical whatever the bytes look like. Only the last step differs, so
 * only the last step is pluggable.
 */
class FormatterTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final UUID RECORD_ID = UUID.fromString("11111111-2222-3333-4444-555555555555");

	private static final List<Column> COLUMNS = List.of(
			new Column("name", "firstName"),
			new Column("score", "score"),
			new Column("active", "active"),
			new Column("mentor", "mentor"));

	private static RecordEnvelope envelope(String payloadJson) {
		try {
			return new RecordEnvelope(MAPPER.readTree("""
					{
					  "recordId": "%s",
					  "contractId": "interns",
					  "recordType": "intern.created",
					  "schemaVersion": 1,
					  "naturalKey": "NK-1",
					  "occurredAt": "2026-07-21T14:10:00Z",
					  "traceId": null,
					  "payload": %s
					}""".formatted(RECORD_ID, payloadJson)));
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static final String PAYLOAD =
			"{\"firstName\": \"Ada\", \"score\": 9.5, \"active\": true}";

	@Test
	void csvIsUnchangedFromWhatThisAdapterHasAlwaysWritten() {
		CsvFormatter formatter = new CsvFormatter();

		assertEquals("record_id,name,score,active,mentor,created_at",
				formatter.header(COLUMNS).orElseThrow());
		assertEquals(RECORD_ID + ",Ada,9.5,true,,2026-07-21T14:10:00Z",
				formatter.format(envelope(PAYLOAD), COLUMNS));
	}

	@Test
	void jsonKeepsTypesThatCsvFlattensToText() {
		// The main reason to choose this format. In CSV every value is text and
		// a loader has to guess; here a number stays a number and a boolean
		// stays a boolean.
		String line = new JsonFormatter(MAPPER).format(envelope(PAYLOAD), COLUMNS);

		assertTrue(line.contains("\"score\":9.5"), line);
		assertTrue(line.contains("\"active\":true"), line);
		assertTrue(line.contains("\"name\":\"Ada\""), line);
		// An omitted field is present and null rather than absent: a key that
		// appears on some lines and not others is what makes a JSON Lines file
		// awkward to load.
		assertTrue(line.contains("\"mentor\":null"), line);
	}

	@Test
	void jsonWritesNoHeaderBecauseAHeaderWouldNotBeJson() {
		// Every reader of a .jsonl file fails on the first line otherwise.
		assertTrue(new JsonFormatter(MAPPER).header(COLUMNS).isEmpty());
		assertTrue(new XmlFormatter().header(COLUMNS).isEmpty());
	}

	@Test
	void jsonLinesAreOneCompleteObjectEach() throws Exception {
		// The property that makes appending safe: a file cut off mid-write is
		// still valid up to its last complete line. A JSON *array* would have to
		// be re-closed on every append.
		String line = new JsonFormatter(MAPPER).format(envelope(PAYLOAD), COLUMNS);
		assertTrue(MAPPER.readTree(line).isObject());
		assertTrue(!line.contains("\n"), "a record must occupy exactly one line");
	}

	@Test
	void xmlEscapesValuesThatWouldOtherwiseBreakTheDocument() {
		String line = new XmlFormatter().format(
				envelope("{\"firstName\": \"Ada & <Lovelace>\"}"), COLUMNS);

		assertTrue(line.contains("<name>Ada &amp; &lt;Lovelace&gt;</name>"), line);
		assertTrue(line.startsWith("<record>") && line.endsWith("</record>"), line);
	}

	@Test
	void aColumnHeaderThatIsNotALegalTagIsMadeOneRatherThanRejected() {
		// "order total" and "1st" are both reasonable column headers and neither
		// is a legal XML element name. Failing the export over a name nobody
		// reads would be a poor trade.
		String line = new XmlFormatter().format(
				envelope("{\"x\": \"v\"}"), List.of(new Column("order total", "x"), new Column("1st", "x")));

		assertTrue(line.contains("<order_total>v</order_total>"), line);
		assertTrue(line.contains("<_1st>v</_1st>"), line);
	}

	@Test
	void anUnknownFormatIsRefusedWithTheListOfRealOnes() {
		RecordFormatters formatters = new RecordFormatters(
				List.of(new CsvFormatter(), new JsonFormatter(MAPPER), new XmlFormatter()));

		assertEquals(List.of("csv", "json", "xml"), formatters.available());

		// The mistake is almost always a typo or a format this deployment does
		// not have, and a message listing the options answers both.
		AttachmentConfigurationException thrown = assertThrows(
				AttachmentConfigurationException.class, () -> formatters.require("yaml"));
		assertTrue(thrown.getMessage().contains("csv"), thrown.getMessage());
	}
}
