package com.iip.fileadapter.schema;

/**
 * Where the envelope schema text comes from.
 *
 * <p>The same two-implementation shape as {@code ContractSource}, for the
 * same reason: production reads the versioned artifact from a running Schema
 * Registry, tests read a fixture from the classpath, and the code that
 * validates never learns which it got.
 */
public interface EnvelopeSchemaSource {

	/** The JSON Schema document, as text. */
	String schemaText();

	/** Human-readable origin, logged once at startup. */
	String describe();
}
