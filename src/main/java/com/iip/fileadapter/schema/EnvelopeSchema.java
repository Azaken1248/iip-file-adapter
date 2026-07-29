package com.iip.fileadapter.schema;

import com.networknt.schema.Error;
import com.networknt.schema.InputFormat;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import tools.jackson.core.JacksonException;

import java.util.Comparator;
import java.util.List;

/**
 * The platform's envelope schema, compiled once and applied to every message
 * (Phase 4.7/4.8).
 *
 * <p>Note the two different things called a registry here. {@link
 * SchemaRegistry} is the JSON Schema library's compiler; the <em>Schema
 * Registry</em> is the running service the schema text was fetched from
 * ({@link EnvelopeSchemaSource}). This class connects them and is the only
 * place where the confusion is possible.
 *
 * <p>This class is a near-copy of the one in the source service and the other
 * adapter, and that is the design rather than an oversight. Extracting it into
 * a shared library would give three independently-deployable services a common
 * compile-time dependency on the very thing they are supposed to agree on only
 * through the registry -- and a library version bump would then be a
 * coordinated release, which is the coupling this platform exists to avoid.
 * What must not drift is the schema, and that is one artifact in one registry.
 *
 * <p>Compilation happens once at construction rather than per message. That
 * is not just a performance choice: an envelope schema that fails to compile
 * must take the service down at startup, not surface as a mysterious
 * per-message failure an hour later.
 */
public class EnvelopeSchema {

	private final Schema schema;
	private final String origin;

	public EnvelopeSchema(EnvelopeSchemaSource source) {
		// Format assertions are off by default in JSON Schema: "format" is
		// an annotation unless asked otherwise. Left off, `recordId` could be
		// any string at all and the uuid/date-time declarations in
		// envelope.json would be documentation rather than rules.
		SchemaRegistry compiler = SchemaRegistry.withDefaultDialect(
				SpecificationVersion.DRAFT_7,
				builder -> builder.schemaRegistryConfig(
						SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build()));

		this.origin = source.describe();
		this.schema = compiler.getSchema(source.schemaText());
	}

	/**
	 * @throws EnvelopeSchemaViolationException listing <em>every</em> problem,
	 *         not just the first. An envelope that is wrong in three ways
	 *         should say so once rather than three deploys running.
	 */
	public void validate(String envelopeJson) {
		List<Error> errors;
		try {
			errors = schema.validate(envelopeJson, InputFormat.JSON);
		}
		catch (JacksonException e) {
			// Bytes that are not JSON at all are a schema violation like any
			// other, and reporting them as one keeps a single failure category
			// for "this message is not an envelope" -- rather than splitting it
			// by how badly wrong the message happened to be.
			throw new EnvelopeSchemaViolationException(List.of("not valid JSON: " + e.getOriginalMessage()));
		}
		if (errors.isEmpty()) {
			return;
		}
		throw new EnvelopeSchemaViolationException(errors.stream()
				.map(error -> error.getInstanceLocation() + " " + error.getMessage())
				.sorted(Comparator.naturalOrder())
				.toList());
	}

	/** Where the schema came from, for the one log line that says so at startup. */
	public String origin() {
		return origin;
	}
}
