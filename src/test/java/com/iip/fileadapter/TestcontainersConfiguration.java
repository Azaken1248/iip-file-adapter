package com.iip.fileadapter;

import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.attachment.AttachmentSource;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
	}

	/**
	 * Phase 6.3. Standing in for the Contract Registry, which is a whole other
	 * service and has no business being started to test this one.
	 *
	 * <p>The {@code interns} attachment reproduces exactly what this adapter did
	 * when its columns were a constant in Java: the same nine fields in the same
	 * order, writing to the same configured file. That is what lets the whole
	 * Release 1-5 suite keep asserting what it always did, and is the evidence
	 * that moving a mapping from code into config changed nothing about it.
	 *
	 * <p>No {@code path} is set, so it falls back to the deployment's configured
	 * output file. Phase 6.3's own test supplies its own attachments with paths,
	 * which is where two contracts writing two different files is proven.
	 */
	@Bean
	AttachmentSource testAttachmentSource() {
		List<Map<String, String>> internColumns = List.of(
				Map.of("header", "intern_id", "field", "internId"),
				Map.of("header", "first_name", "field", "firstName"),
				Map.of("header", "last_name", "field", "lastName"),
				Map.of("header", "email", "field", "email"),
				Map.of("header", "college", "field", "college"),
				Map.of("header", "department", "field", "department"),
				Map.of("header", "mentor", "field", "mentor"),
				Map.of("header", "start_date", "field", "startDate"),
				Map.of("header", "status", "field", "status"));

		return new AttachmentSource() {

			@Override
			public List<Attachment> loadAll() {
				return List.of(new Attachment(
						"11111111-1111-1111-1111-111111111111",
						"interns",
						Map.of("columns", internColumns)));
			}

			@Override
			public String describe() {
				return "a fixed test attachment (interns -> the configured output file)";
			}
		};
	}
}
