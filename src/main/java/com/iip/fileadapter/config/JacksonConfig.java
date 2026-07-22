package com.iip.fileadapter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot 4's own default Jackson is v3 (tools.jackson.core); this is
 * the Jackson 2.x ObjectMapper the Kafka message body is parsed with by
 * hand (InternCreatedConsumer) and DLQ envelopes are serialized with
 * (DlqPublisher) -- java.time support needs JavaTimeModule registered
 * explicitly either way.
 */
@Configuration
public class JacksonConfig {

	@Bean
	public ObjectMapper objectMapper() {
		return new ObjectMapper().registerModule(new JavaTimeModule());
	}
}
