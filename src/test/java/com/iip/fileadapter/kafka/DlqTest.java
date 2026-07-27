package com.iip.fileadapter.kafka;

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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * "A poison message is quarantined instead of blocking the pipeline"
 * (Implementation Plan §4, Release 1 backlog) -- publishes malformed JSON
 * (not deserializable at all, so the FailureClassifier's NON_RETRIABLE
 * path short-circuits straight to the DLQ without retrying) and confirms
 * a subsequent well-formed message still processes normally afterward.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class DlqTest {

	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.resolve("interns.csv").toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
	}

	@Autowired
	private KafkaContainer kafkaContainer;

	private void publish(String key, String json) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			producer.send(new ProducerRecord<>("interns.created", key, json)).get();
		} catch (Exception e) {
			fail("failed to publish test message: " + e.getMessage());
		}
	}

	@Test
	void aMalformedMessageLandsInTheDlqAndASubsequentGoodMessageStillProcesses() throws IOException, InterruptedException {
		String poisonKey = "INT-POISON-" + UUID.randomUUID();
		publish(poisonKey, "{ this is not valid json ");

		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-consumer-" + UUID.randomUUID());
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		boolean foundOnDlq = false;
		try (Consumer<String, String> dlqConsumer = new KafkaConsumer<>(consumerProps)) {
			dlqConsumer.subscribe(List.of("iip.dlq"));
			for (int i = 0; i < 20 && !foundOnDlq; i++) {
				var records = dlqConsumer.poll(Duration.ofMillis(500));
				for (ConsumerRecord<String, String> r : records) {
					if (poisonKey.equals(r.key())) {
						foundOnDlq = true;
						assertTrue(r.value().contains("\"originalPayload\""), "envelope should carry the original payload");
						assertTrue(r.value().contains("SCHEMA_VIOLATION"), "malformed JSON should classify as SCHEMA_VIOLATION");
						assertTrue(r.value().contains("\"failedAdapter\":\"file-adapter\""));
					}
				}
			}
		}
		assertTrue(foundOnDlq, "expected the malformed message to land on iip.dlq within the poll window");

		// A subsequent good message on the same topic should still process
		// normally -- the poison message didn't stall the pipeline.
		String goodInternId = "INT-AFTER-POISON-" + UUID.randomUUID();
		String recordId = UUID.randomUUID().toString();
		String goodJson = envelopeJson(recordId, goodInternId, "Ada", "Lovelace",
				"ada@example.com", "MIT", "Platform Engineering", "Sam");
		publish(goodInternId, goodJson);

		Path csvPath = tempDir.resolve("interns.csv");
		boolean found = false;
		for (int i = 0; i < 20 && !found; i++) {
			Thread.sleep(500);
			if (Files.exists(csvPath)) {
				found = Files.readAllLines(csvPath).stream().anyMatch(line -> line.contains(goodInternId));
			}
		}
		assertTrue(found, "a good message after a poison one should still be processed");
	}
}
