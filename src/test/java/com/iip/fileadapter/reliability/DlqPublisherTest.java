package com.iip.fileadapter.reliability;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Covers errorType() in isolation -- publish()'s actual network behavior
 * (and the envelope's full shape on the wire) is already proven by the
 * Testcontainers-based DlqTest; this is specifically about the label
 * decision, which doesn't need a broker to exercise. kafkaTemplate is
 * null throughout since errorType() never touches it.
 */
class DlqPublisherTest {

	private final DlqPublisher publisher = new DlqPublisher(null, new ObjectMapper(), "intern.dlq", new FailureClassifier());

	@Test
	void anExhaustedRetriableFailureIsLabeledRetryExhausted() {
		assertEquals("RETRY_EXHAUSTED", publisher.errorType(new ConnectException("Connection refused")));
	}

	@Test
	void malformedJsonIsLabeledSchemaViolation() {
		assertEquals("SCHEMA_VIOLATION", publisher.errorType(new JsonParseException(null, "Unexpected character")));
	}

	@Test
	void anUnrecognizedNonRetriableFailureIsNotMislabeledAsRetryExhausted() {
		// Regression test (Phase 2.4 audit): before the fix, errorType()
		// only looked at exception *type*, never whether the failure had
		// actually been retried -- so an exception it didn't recognize at
		// all (immediately non-retriable, per FailureClassifier's default)
		// still came out "RETRY_EXHAUSTED" on the very first attempt. That
		// misdiagnoses a fundamentally bad message as a target that kept
		// failing when it was never retried at all.
		String errorType = publisher.errorType(new IllegalStateException("something we've never seen"));
		assertNotEquals("RETRY_EXHAUSTED", errorType,
				"a failure that was never retried must not be labeled as if retries were exhausted");
		assertEquals("UNCLASSIFIED_FAILURE", errorType);
	}
}
