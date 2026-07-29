package com.iip.fileadapter.schema;

import java.util.List;

/**
 * Thrown when a consumed message does not conform to the registered envelope
 * schema (Phase 4.8).
 *
 * <p>Deliberately non-retriable, and `FailureClassifier` says so explicitly
 * rather than relying on its unknown-type default: replaying identical bytes
 * against an identical schema can only fail identically, so the three attempts
 * would be three guaranteed failures and a delay before the DLQ entry that was
 * always coming.
 *
 * <p>Reaching this class at all means the platform's own producer wrote
 * something it should not have -- Phase 4.8 validates on the way out too. That
 * is exactly why the check is worth paying for on the way in: the guarantee an
 * adapter offers should not rest on another service's correctness.
 */
public class EnvelopeSchemaViolationException extends RuntimeException {

	private final transient List<String> violations;

	public EnvelopeSchemaViolationException(List<String> violations) {
		super("message does not conform to the registered envelope schema: " + String.join("; ", violations));
		this.violations = List.copyOf(violations);
	}

	public List<String> violations() {
		return violations;
	}
}
