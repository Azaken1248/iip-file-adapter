package com.iip.fileadapter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot 4's own default Jackson is v3 (tools.jackson.core); this is
 * the Jackson 2.x ObjectMapper the Kafka message body is parsed with by
 * hand (InternCreatedConsumer) and DLQ envelopes are serialized with
 * (DlqPublisher) -- java.time support needs JavaTimeModule registered
 * explicitly either way.
 *
 * WRITE_DATES_AS_TIMESTAMPS is disabled for the same reason it's disabled
 * on the Kafka producer's own ObjectMapper (see the Spring Boot 4 / Jackson
 * 3 gotchas table in docs/05-phased-rollout.md): without it, quarantinedAt/
 * replayedAt on a DlqEnvelope serialize as raw epoch-second doubles instead
 * of the ISO-8601 strings docs/03-data-model.md's DLQ envelope actually
 * specifies -- a real bug this bean shipped with, caught by db-adapter's
 * Phase 2.4 envelope-completeness audit (see its JacksonConfig) and fixed
 * here too since this adapter's bean mirrors it exactly.
 */
@Configuration
public class JacksonConfig {

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper()
				.registerModule(new JavaTimeModule())
				.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	}
}
