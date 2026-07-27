package com.iip.fileadapter.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.TestcontainersConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase 2.4's audit of the DLQ envelope (docs/03-data-model.md §3): proves
 * every field lands correctly on the wire (parsed as real JSON, not
 * substring checks), and specifically exercises the RETRY_EXHAUSTED path,
 * which Phase 1.19's DlqTest never covered -- that test only ever produced
 * an immediate SCHEMA_VIOLATION (malformed JSON, never retried). The
 * output path is pointed at an existing *directory* rather than a file --
 * Files.writeString on a directory fails with a plain FileSystemException
 * ("Is a directory"), which is a persistent, always-retriable-per-
 * classifier failure that never recovers on its own, exactly what's needed
 * to force a genuine exhaustion rather than a one-off transient blip.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class DlqEnvelopeTest {

	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileAndRetryProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
		registry.add("iip.retry.max-attempts", () -> "3");
		registry.add("iip.retry.initial-backoff-ms", () -> "50");
		registry.add("iip.retry.multiplier", () -> "1.0");
	}

	@Autowired
	private KafkaContainer kafkaContainer;

	private final ObjectMapper objectMapper = new ObjectMapper();

	private void publish(String key, String json) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			producer.send(new ProducerRecord<>("intern.created", key, json)).get();
		} catch (Exception e) {
			fail("failed to publish test message: " + e.getMessage());
		}
	}

	@Test
	void aRetryExhaustedFailureProducesACompleteAndCorrectlyLabeledEnvelope() throws Exception {
		String internId = "INT-DLQ-EXHAUSTED-" + UUID.randomUUID();
		String recordId = UUID.randomUUID().toString();
		String json = envelopeJson(recordId, internId, "Katherine", "Johnson",
				"katherine@example.com", "West Virginia State", "Orbital Mechanics", "Dorothy");

		publish(internId, json);
		JsonNode envelope = pollForDlqEntry(internId);

		assertTrue(envelope != null, "expected the message to land on intern.dlq once its retry budget was exhausted");

		assertEquals("intern.created", envelope.get("originalTopic").asText());
		assertTrue(envelope.get("originalPartition").asInt() >= 0);
		assertTrue(envelope.get("originalOffset").asLong() >= 0);
		assertEquals(internId, envelope.get("originalKey").asText());
		assertTrue(envelope.get("originalPayload").asText().contains(recordId),
				"the original payload must be preserved verbatim, not summarized or dropped");
		assertEquals("RETRY_EXHAUSTED", envelope.get("errorType").asText(),
				"a persistently failing write is a genuine exhaustion, not an immediate non-retriable failure");
		assertFalse(envelope.get("errorMessage").asText().isBlank());
		assertEquals("file-adapter", envelope.get("failedAdapter").asText());
		assertEquals(3, envelope.get("attemptCount").asInt(), "expected exactly the configured max-attempts");
		assertTrue(Instant.parse(envelope.get("quarantinedAt").asText()).isAfter(Instant.now().minusSeconds(60)),
				"quarantinedAt must be a real ISO-8601 instant, not a raw epoch number");
		assertFalse(envelope.get("replayed").asBoolean());
		assertTrue(envelope.get("replayedAt").isNull());
	}

	private JsonNode pollForDlqEntry(String key) throws Exception {
		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-consumer-" + UUID.randomUUID());
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try (Consumer<String, String> dlqConsumer = new KafkaConsumer<>(consumerProps)) {
			dlqConsumer.subscribe(List.of("intern.dlq"));
			for (int i = 0; i < 20; i++) {
				var records = dlqConsumer.poll(Duration.ofMillis(500));
				for (ConsumerRecord<String, String> r : records) {
					if (key.equals(r.key())) {
						return objectMapper.readTree(r.value());
					}
				}
			}
		}
		return null;
	}
}
