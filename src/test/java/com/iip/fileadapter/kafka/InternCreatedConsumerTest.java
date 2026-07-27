package com.iip.fileadapter.kafka;

import com.iip.fileadapter.TestcontainersConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Publishes raw JSON messages shaped exactly like what source-service
 * actually puts on the wire (see docs/03-data-model.md §1) directly onto
 * interns.created -- not via any shared Java type, since file-adapter must
 * never depend on source-service's classes; the canonical JSON contract is
 * the only thing crossing the boundary.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class InternCreatedConsumerTest {

	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.resolve("interns.csv").toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
	}

	@Autowired
	private KafkaContainer kafkaContainer;

	private String eventJson(String recordId, String internId) {
		return envelopeJson(recordId, internId, "Grace", "Hopper",
				"grace@example.com", "Yale", "Compilers", "Howard");
	}

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

	private List<String> pollCsvLinesContaining(String internId) throws IOException, InterruptedException {
		Path csvPath = tempDir.resolve("interns.csv");
		for (int i = 0; i < 20; i++) {
			Thread.sleep(500);
			if (Files.exists(csvPath)) {
				List<String> lines = Files.readAllLines(csvPath).stream()
						.filter(line -> line.contains(internId))
						.toList();
				if (!lines.isEmpty()) {
					return lines;
				}
			}
		}
		return List.of();
	}

	@Test
	void consumingOneMessageResultsInOneCsvLine() throws Exception {
		String internId = "INT-FILE-" + UUID.randomUUID();
		publish(internId, eventJson(UUID.randomUUID().toString(), internId));

		List<String> lines = pollCsvLinesContaining(internId);

		assertEquals(1, lines.size(), "expected exactly one CSV line for " + internId);
	}

	@Test
	void deliveringTheSameMessageTwiceResultsInExactlyOneCsvLine() throws Exception {
		String internId = "INT-FILE-DUP-" + UUID.randomUUID();
		String json = eventJson(UUID.randomUUID().toString(), internId);

		publish(internId, json);
		publish(internId, json);

		List<String> lines = pollCsvLinesContaining(internId);
		// give a possible second delivery a moment to land too
		Thread.sleep(2000);
		lines = Files.readAllLines(tempDir.resolve("interns.csv")).stream()
				.filter(line -> line.contains(internId))
				.toList();

		assertEquals(1, lines.size(), "expected exactly one CSV line for " + internId + " after duplicate delivery");
	}
}
