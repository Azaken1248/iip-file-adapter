package com.iip.fileadapter.schema;

import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Reads the envelope schema from {@code schemas/envelope.json} on the
 * classpath.
 *
 * <p>Only test resources carry that file. The packaged jar deliberately does
 * not, so a deployment that forgets {@code SCHEMA_REGISTRY_URL} and falls back
 * to this source dies at startup saying the schema is missing, rather than
 * quietly processing unvalidated messages -- the same failure mode chosen for
 * contracts in Phase 4.4, for the same reason.
 */
public class ClasspathEnvelopeSchemaSource implements EnvelopeSchemaSource {

	static final String LOCATION = "schemas/envelope.json";

	@Override
	public String schemaText() {
		ClassPathResource resource = new ClassPathResource(LOCATION);
		if (!resource.exists()) {
			throw new IllegalStateException(
					"no envelope schema found at classpath:" + LOCATION
							+ " -- set SCHEMA_REGISTRY_URL and iip.envelope-schema.source=registry to fetch "
							+ "the registered schema, which is how every deployment is expected to run");
		}
		try (InputStream in = resource.getInputStream()) {
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		catch (IOException e) {
			throw new UncheckedIOException("could not read classpath:" + LOCATION, e);
		}
	}

	@Override
	public String describe() {
		return "classpath:" + LOCATION;
	}
}
