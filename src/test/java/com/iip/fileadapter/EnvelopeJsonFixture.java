package com.iip.fileadapter;

/**
 * Builds the canonical envelope JSON that source-service publishes
 * (docs/03-data-model.md §1a/§1b), for tests that put a message on a real
 * topic.
 *
 * <p>Introduced by Phase 3.2. Every consumer test previously inlined its own
 * copy of the flat Release 1 wire shape, so the envelope split would
 * otherwise have meant editing the same JSON literal in four places -- and
 * editing it again in Release 6, and again in Release 7. The shape lives
 * here once; the tests keep their own assertions.
 *
 * <p>Deliberately a hand-written string rather than a serialized Java type:
 * these tests exist to prove this service parses the <em>JSON contract</em>
 * correctly, and generating the fixture from a local record would only prove
 * the record round-trips through itself.
 */
public final class EnvelopeJsonFixture {

	private EnvelopeJsonFixture() {
	}

	public static String envelopeJson(
			String recordId,
			String internId,
			String firstName,
			String lastName,
			String email,
			String college,
			String department,
			String mentor) {
		return """
				{
				  "recordId": "%s",
				  "contractId": "interns",
				  "recordType": "intern.created",
				  "schemaVersion": 1,
				  "naturalKey": "%s",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "traceId": null,
				  "payload": {
				    "internId": "%s",
				    "firstName": "%s",
				    "lastName": "%s",
				    "email": "%s",
				    "college": "%s",
				    "department": "%s",
				    "mentor": "%s",
				    "startDate": "2026-09-01",
				    "status": "ACTIVE"
				  }
				}
				""".formatted(recordId, internId, internId, firstName, lastName, email, college, department, mentor);
	}
	/**
	 * An envelope for <em>any</em> contract, with the payload supplied as raw
	 * JSON (Phase 6.10).
	 *
	 * <p>The payload is a string rather than a typed object because the whole
	 * point of the contracts it is used for is that this repository has no Java
	 * type for them -- building one from a record would quietly reintroduce the
	 * type it exists to prove unnecessary.
	 */
	public static String envelopeJson(
			String recordId,
			String contractId,
			String recordType,
			String naturalKey,
			String payloadJson) {
		return """
				{
				  "recordId": "%s",
				  "contractId": "%s",
				  "recordType": "%s",
				  "schemaVersion": 1,
				  "naturalKey": "%s",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "traceId": null,
				  "payload": %s
				}
				""".formatted(recordId, contractId, recordType, naturalKey, payloadJson);
	}
}
