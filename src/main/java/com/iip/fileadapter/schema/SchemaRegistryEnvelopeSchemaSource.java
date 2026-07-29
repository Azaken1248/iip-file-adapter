package com.iip.fileadapter.schema;

import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Fetches the envelope schema from a Confluent-compatible Schema Registry
 * (Phase 4.7).
 *
 * <p>Plain HTTP against the registry's REST API rather than Confluent's
 * client library, and plain canonical JSON on the wire rather than Confluent's
 * wire format. The registry is used for what it is good at -- being the one
 * versioned, compatibility-checked home of the envelope -- without making
 * every consumer's bytes depend on it. That keeps the property held since
 * Phase 1.12: an adapter reads canonical JSON off a topic and needs no shared
 * artifact and no registry lookup to do it. See docs/01-architecture.md.
 *
 * <p>The schema is fetched once, at startup. It does not need a refresh
 * interval the way contracts do: contracts change through a control-plane UI
 * many times a day by design, whereas an envelope change is a platform-wide
 * event that goes through CI and a deploy (Data Model 5.1). Re-reading it on a
 * timer would add a way for the running fleet to change behaviour with nobody
 * deploying anything.
 */
public class SchemaRegistryEnvelopeSchemaSource implements EnvelopeSchemaSource {

	private final RestClient restClient;
	private final String baseUrl;
	private final String subject;

	public SchemaRegistryEnvelopeSchemaSource(RestClient.Builder restClientBuilder, String baseUrl, String subject) {
		this.baseUrl = baseUrl;
		this.subject = subject;
		this.restClient = restClientBuilder.baseUrl(baseUrl).build();
	}

	@Override
	public String schemaText() {
		RegisteredSchema registered;
		try {
			registered = restClient.get()
					.uri("/subjects/{subject}/versions/latest", subject)
					.retrieve()
					.body(RegisteredSchema.class);
		}
		catch (RestClientException e) {
			// Not swallowed and not defaulted to "no validation". A service
			// that cannot obtain the schema cannot honour the guarantee this
			// phase exists to provide, and starting anyway would mean
			// publishing unvalidated envelopes under a name that promises
			// otherwise.
			throw new IllegalStateException(
					"could not fetch subject '" + subject + "' from the schema registry at " + baseUrl
							+ ": " + e.getMessage(), e);
		}

		if (registered == null || registered.schema() == null || registered.schema().isBlank()) {
			throw new IllegalStateException(
					"the schema registry at " + baseUrl + " returned no schema for subject '" + subject
							+ "' -- has schema-registry-init run?");
		}
		return registered.schema();
	}

	@Override
	public String describe() {
		return baseUrl + " subject '" + subject + "'";
	}

	/**
	 * The registry carries the schema document as an escaped string inside its
	 * response, not as nested JSON; every other field of the response (id,
	 * version, schemaType) is metadata this class has no use for.
	 */
	record RegisteredSchema(String schema) {
	}
}
