package com.iip.fileadapter.config;

import com.iip.fileadapter.attachment.AttachmentRegistry;
import com.iip.fileadapter.attachment.AttachmentSource;
import com.iip.fileadapter.attachment.FixedAttachmentSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the generic adapter pipeline (Phase 6.2).
 *
 * <p>{@link ConditionalOnMissingBean} on the source for the same reason the
 * db-adapter does it: a test can supply its own attachments and still exercise
 * the real registry -- refresh, wholesale swap, logging -- rather than a mock
 * of it. It is also how Phase 6.3 will substitute the registry-backed source
 * without touching anything else in this file.
 */
@Configuration
public class PipelineConfig {

	@Bean
	@ConditionalOnMissingBean(AttachmentSource.class)
	AttachmentSource fixedAttachmentSource(@Value("${iip.attachments.contract-id}") String contractId) {
		return new FixedAttachmentSource(contractId);
	}

	@Bean
	AttachmentRegistry attachmentRegistry(AttachmentSource source) {
		return new AttachmentRegistry(source);
	}
}
