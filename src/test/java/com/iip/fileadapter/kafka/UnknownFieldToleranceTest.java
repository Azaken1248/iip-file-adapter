package com.iip.fileadapter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.config.JacksonConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Phase 4.11's premise, at the one place it can actually fail: <strong>an
 * optional field added to a contract must not break an adapter nobody
 * redeployed.</strong>
 *
 * <p>Data Model 5.2 calls adding an optional field the safe, ordinary
 * evolution. That is only true if deployed consumers tolerate a field their
 * mapping has never heard of -- and Jackson's default is the opposite:
 * {@code FAIL_ON_UNKNOWN_PROPERTIES} is <em>enabled</em> unless something
 * turns it off, so a hand-built {@code new ObjectMapper()} rejects exactly the
 * change the platform advertises as safe. It would have surfaced as a DLQ
 * entry per record, minutes after a contract edit that the registry, the
 * compatibility gate, and the source service had all just approved.
 *
 * <p>The asymmetry with the source service is deliberate, not an
 * inconsistency. There, an undeclared payload key is rejected: it is almost
 * always a typo or a client running ahead of the schema, and accepting it
 * would put silent data loss behind a 202. Here, an undeclared key means the
 * contract moved ahead of this adapter's mapping, which is a normal and
 * expected state of a system whose services deploy independently. Producer
 * strict, consumer tolerant -- the same rule the envelope schema follows by
 * leaving {@code additionalProperties} open.
 */
class UnknownFieldToleranceTest {

	private final ObjectMapper objectMapper = new JacksonConfig().objectMapper();

	@Test
	void aPayloadFieldThisAdapterHasNeverHeardOfIsIgnoredRatherThanRejected() {
		String envelopeWithANewerContractsField = """
				{
				  "recordId": "b8b2f1a4-1f7c-4a51-9b47-2f3f1b7d9c10",
				  "contractId": "interns",
				  "recordType": "intern.created",
				  "schemaVersion": 2,
				  "naturalKey": "INT001",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "traceId": null,
				  "payload": {
				    "internId": "INT001",
				    "firstName": "Grace",
				    "lastName": "Hopper",
				    "email": "grace@example.com",
				    "college": "Yale",
				    "department": "Platform",
				    "mentor": "Sam",
				    "startDate": "2026-09-01",
				    "status": "ACTIVE",
				    "githubHandle": "gracehopper"
				  }
				}""";

		CanonicalEnvelope envelope = assertDoesNotThrow(() ->
				objectMapper.readValue(envelopeWithANewerContractsField, CanonicalEnvelope.class));

		// Ignored, and everything this adapter does map still arrives intact --
		// the point is tolerance, not that the record is silently degraded.
		assertEquals("INT001", envelope.payload().internId());
		assertEquals("Grace", envelope.payload().firstName());
		assertEquals("grace@example.com", envelope.payload().email());
		assertEquals(2, envelope.schemaVersion());
	}

	/**
	 * The same rule one level up. A new <em>envelope</em> field is rarer and
	 * governed by a stricter process, but BACKWARD exists precisely so the
	 * source service can ship one before the adapters are updated -- and the
	 * envelope schema leaves {@code additionalProperties} open for that reason.
	 * A deserializer that rejected the field would make the schema's tolerance
	 * meaningless.
	 */
	@Test
	void anEnvelopeFieldThisAdapterHasNeverHeardOfIsAlsoIgnored() {
		String envelopeFromANewerSourceService = """
				{
				  "recordId": "b8b2f1a4-1f7c-4a51-9b47-2f3f1b7d9c10",
				  "contractId": "interns",
				  "recordType": "intern.created",
				  "schemaVersion": 1,
				  "naturalKey": "INT001",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "traceId": null,
				  "producedBy": "source-service@2.0.0",
				  "payload": {
				    "internId": "INT001",
				    "firstName": "Grace",
				    "lastName": "Hopper",
				    "email": "grace@example.com",
				    "college": "Yale",
				    "department": "Platform",
				    "mentor": "Sam",
				    "startDate": "2026-09-01",
				    "status": "ACTIVE"
				  }
				}""";

		CanonicalEnvelope envelope = assertDoesNotThrow(() ->
				objectMapper.readValue(envelopeFromANewerSourceService, CanonicalEnvelope.class));

		assertEquals("INT001", envelope.naturalKey());
	}
}
