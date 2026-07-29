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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase 4.8's done-when on the consuming side: <strong>a deliberately
 * non-conforming envelope is rejected at the boundary, not deep in business
 * logic.</strong>
 *
 * <p>The messages below are all <em>well-formed JSON</em>, which is what makes
 * this different from {@code DlqTest}. Unparseable bytes were always caught;
 * these are plausible-looking envelopes that are wrong in ways only the schema
 * knows about, and before this phase each would have travelled into the dedup
 * guard and the CSV writer before failing -- or worse, not failed at all.
 *
 * <p>The dedup store is the reason ordering matters here. It is keyed on
 * {@code recordId}, so validating after it would let a malformed message
 * record itself as seen, and a corrected replay of the same record would then
 * be silently dropped as a duplicate.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class EnvelopeValidationAtTheBoundaryTest {

	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.resolve("interns.csv").toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
	}

	@Autowired
	private KafkaContainer kafkaContainer;

	@Test
	void anEnvelopeWithNoRecordIdIsQuarantinedBeforeTheDedupGuard() {
		String internId = "INT-NO-RECORD-ID-" + UUID.randomUUID();
		publish(internId, """
				{
				  "contractId": "interns",
				  "recordType": "intern.created",
				  "schemaVersion": 1,
				  "naturalKey": "%s",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "payload": {"internId": "%s", "firstName": "Ada", "lastName": "Lovelace",
				              "email": "ada@example.com", "college": "MIT",
				              "department": "Platform", "startDate": "2026-09-01", "status": "ACTIVE"}
				}""".formatted(internId, internId));

		assertQuarantined(internId, "recordId");
		assertNotWrittenToCsv(internId);
	}

	@Test
	void anEnvelopeWhosePayloadIsNotAnObjectIsQuarantinedBeforeTheCsvWriter() {
		String internId = "INT-SCALAR-PAYLOAD-" + UUID.randomUUID();
		publish(internId, """
				{
				  "recordId": "%s",
				  "contractId": "interns",
				  "recordType": "intern.created",
				  "schemaVersion": 1,
				  "naturalKey": "%s",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "payload": "not an object"
				}""".formatted(UUID.randomUUID(), internId));

		assertQuarantined(internId, "payload");
		assertNotWrittenToCsv(internId);
	}

	@Test
	void anEnvelopeWithAnEmptyNaturalKeyIsQuarantined() {
		// Published under a non-empty Kafka key so the DLQ entry is findable;
		// what is malformed is the envelope's own naturalKey field, which is
		// what every downstream consumer partitions and correlates on.
		String key = "INT-EMPTY-KEY-" + UUID.randomUUID();
		publish(key, """
				{
				  "recordId": "%s",
				  "contractId": "interns",
				  "recordType": "intern.created",
				  "schemaVersion": 1,
				  "naturalKey": "",
				  "occurredAt": "2026-07-21T14:10:00Z",
				  "payload": {"internId": "%s", "firstName": "Ada", "lastName": "Lovelace",
				              "email": "ada@example.com", "college": "MIT",
				              "department": "Platform", "startDate": "2026-09-01", "status": "ACTIVE"}
				}""".formatted(UUID.randomUUID(), key));

		assertQuarantined(key, "naturalKey");
		assertNotWrittenToCsv(key);
	}

	// --- helpers -----------------------------------------------------------

	private void publish(String key, String json) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			producer.send(new ProducerRecord<>("interns.created", key, json)).get();
		}
		catch (Exception e) {
			fail("failed to publish test message: " + e.getMessage());
		}
	}

	/**
	 * The DLQ entry has to say <em>which</em> field was wrong. An operator
	 * looking at a quarantined message needs to know whether to fix a producer
	 * or roll a schema back, and "invalid envelope" answers neither question.
	 */
	private void assertQuarantined(String key, String expectedFieldInReason) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "envelope-validation-test-" + UUID.randomUUID());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try (Consumer<String, String> dlq = new KafkaConsumer<>(props)) {
			dlq.subscribe(List.of("iip.dlq"));
			for (int i = 0; i < 30; i++) {
				for (ConsumerRecord<String, String> record : dlq.poll(Duration.ofMillis(500))) {
					if (key.equals(record.key())) {
						assertTrue(record.value().contains("SCHEMA_VIOLATION"),
								"an envelope that fails the schema is a schema violation: " + record.value());
						assertTrue(record.value().contains(expectedFieldInReason),
								"the DLQ entry should name " + expectedFieldInReason + ": " + record.value());
						assertTrue(record.value().contains("\"attemptCount\":1"),
								"a schema violation must not be retried -- identical bytes against an identical "
										+ "schema fail identically: " + record.value());
						assertTrue(record.value().contains("\"failedAdapter\":\"file-adapter\""), record.value());
						return;
					}
				}
			}
		}
		fail("expected " + key + " to be quarantined on iip.dlq");
	}

	private void assertNotWrittenToCsv(String internId) {
		Path csv = tempDir.resolve("interns.csv");
		if (!Files.exists(csv)) {
			return;
		}
		try {
			assertTrue(Files.readAllLines(csv).stream().noneMatch(line -> line.contains(internId)),
					"a rejected envelope must not have reached the CSV -- that is what 'at the boundary' means");
		}
		catch (IOException e) {
			fail("could not read the output CSV: " + e.getMessage());
		}
	}
}
