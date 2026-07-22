package com.iip.fileadapter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.kafka.InternCreatedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * Mirrors db-adapter's KafkaConsumerConfig: Spring Boot's default consumer
 * autoconfiguration builds JsonDeserializer via no-arg reflection (Jackson
 * 3, no java.time support), and by default trusts the producer's
 * __TypeId__ header to pick a target class -- which here would be
 * source-service's own (inaccessible) CanonicalInternRecord, not this
 * service's InternCreatedEvent. Ignoring type headers and always
 * deserializing to InternCreatedEvent also keeps this adapter decoupled
 * from source-service's Java types, matching the canonical JSON-only
 * contract (docs/01-architecture.md AD-1).
 */
@Configuration
public class KafkaConsumerConfig {

	@Bean
	@SuppressWarnings("removal")
	public DefaultKafkaConsumerFactoryCustomizer jsonValueDeserializerCustomizer() {
		ObjectMapper objectMapper = new ObjectMapper()
				.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

		JsonDeserializer<InternCreatedEvent> deserializer =
				new JsonDeserializer<>(InternCreatedEvent.class, objectMapper).ignoreTypeHeaders();

		return consumerFactory -> setValueDeserializer(consumerFactory, deserializer);
	}

	// DefaultKafkaConsumerFactoryCustomizer's parameter is <?, ?>, and the
	// wildcard's captured value type can't be proven equal to
	// InternCreatedEvent by the compiler even though it always is at
	// runtime for this specific factory -- a generic helper method lets
	// capture conversion bind K/V properly instead of fighting the
	// wildcard directly.
	@SuppressWarnings("unchecked")
	private <K, V> void setValueDeserializer(
			DefaultKafkaConsumerFactory<K, V> consumerFactory, JsonDeserializer<InternCreatedEvent> deserializer) {
		consumerFactory.setValueDeserializer((Deserializer<V>) deserializer);
	}
}
