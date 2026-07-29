package com.iip.fileadapter.config;

import com.iip.fileadapter.schema.ClasspathEnvelopeSchemaSource;
import com.iip.fileadapter.schema.EnvelopeSchema;
import com.iip.fileadapter.schema.EnvelopeSchemaSource;
import com.iip.fileadapter.schema.SchemaRegistryEnvelopeSchemaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

/**
 * Chooses where the envelope schema comes from (Phase 4.7/4.8).
 *
 * <p>The adapter fetches the schema itself rather than trusting the producer
 * to have validated. That is the same reasoning as the idempotency guard: a
 * guarantee an adapter offers has to hold even when the thing upstream of it
 * is wrong, because "upstream is correct" is not something this service can
 * check at 3am.
 */
@Configuration
public class EnvelopeSchemaConfig {

	private static final Logger log = LoggerFactory.getLogger(EnvelopeSchemaConfig.class);

	@Bean
	@ConditionalOnProperty(name = "iip.envelope-schema.source", havingValue = "classpath", matchIfMissing = true)
	EnvelopeSchemaSource classpathEnvelopeSchemaSource() {
		return new ClasspathEnvelopeSchemaSource();
	}

	@Bean
	@ConditionalOnProperty(name = "iip.envelope-schema.source", havingValue = "registry")
	EnvelopeSchemaSource schemaRegistryEnvelopeSchemaSource(
			RestClient.Builder restClientBuilder,
			@Value("${iip.envelope-schema.registry-url}") String registryUrl,
			@Value("${iip.envelope-schema.subject}") String subject) {

		return new SchemaRegistryEnvelopeSchemaSource(restClientBuilder, registryUrl, subject);
	}

	@Bean
	EnvelopeSchema envelopeSchema(EnvelopeSchemaSource source) {
		EnvelopeSchema schema = new EnvelopeSchema(source);
		log.info("envelope schema loaded from {}", schema.origin());
		return schema;
	}
}
