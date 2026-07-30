package com.iip.fileadapter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.catalog.AdapterTypeRegistrar;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

/**
 * Wires this adapter's announcement of itself to the type catalog (Phase 6.1).
 *
 * <p>Note what is <em>not</em> here, and will be in Phase 6.3: an attachment
 * read path. This adapter still learns which topic to read and where to write
 * from its own configuration, so at this point it can tell the control plane
 * that a {@code csv} type exists without yet being able to act on an attachment
 * to one. Registering anyway is deliberate -- the catalog is what the attach
 * form is built from, and building that form is what makes the gap visible.
 */
@Configuration
@EnableScheduling
public class CatalogConfig {

	@Bean
	AdapterTypeRegistrar adapterTypeRegistrar(
			RestClient.Builder restClientBuilder,
			@Value("${iip.catalog.registry-url}") String registryUrl,
			@Value("classpath:adapter-type.json") Resource descriptor,
			ObjectMapper objectMapper) {

		return new AdapterTypeRegistrar(restClientBuilder, registryUrl, descriptor, objectMapper);
	}
}
