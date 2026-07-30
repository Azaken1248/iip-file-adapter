package com.iip.fileadapter.config;

import com.iip.fileadapter.attachment.AttachmentRegistry;
import com.iip.fileadapter.attachment.AttachmentSource;
import com.iip.fileadapter.attachment.RegistryAttachmentSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestClient;

/**
 * Wires the generic adapter pipeline (Phase 6.2) and its attachment read path
 * (Phase 6.3).
 *
 * <p>{@link ConditionalOnMissingBean} on the source for the same reason the
 * db-adapter does it: a test can supply its own attachments and still exercise
 * the real registry -- refresh, wholesale swap, logging -- rather than a mock
 * of it.
 */
@Configuration
@EnableScheduling
public class PipelineConfig {

	@Bean
	@ConditionalOnMissingBean(AttachmentSource.class)
	AttachmentSource registryAttachmentSource(
			RestClient.Builder restClientBuilder,
			@Value("${iip.attachments.registry-url}") String registryUrl) {

		return new RegistryAttachmentSource(restClientBuilder, registryUrl);
	}

	@Bean
	AttachmentRegistry attachmentRegistry(AttachmentSource source) {
		return new AttachmentRegistry(source);
	}
}
