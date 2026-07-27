package com.iip.fileadapter.api;

import com.iip.fileadapter.TestcontainersConfiguration;
import com.iip.fileadapter.csv.InternRow;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Mirrors db-adapter's AdminControllerTest: proves a message published
 * while the listener is paused isn't lost -- it just isn't processed
 * *yet* -- and lands correctly once resumed.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
class AdminControllerTest {

	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.resolve("interns.csv").toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
	}

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private KafkaContainer kafkaContainer;

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
	void listInternsReturnsWhatsInTheCsvFile() throws InterruptedException {
		String internId = "INT-ADMIN-LIST-" + UUID.randomUUID();
		String recordId = UUID.randomUUID().toString();
		publish(internId, envelopeJson(recordId, internId, "Ada", "Lovelace",
				"ada@example.com", "MIT", "Platform Engineering", "Sam"));

		Optional<InternRow> found = Optional.empty();
		for (int i = 0; i < 20 && found.isEmpty(); i++) {
			Thread.sleep(500);
			InternRow[] rows = restTemplate.getForObject("/interns", InternRow[].class);
			found = Arrays.stream(rows).filter(row -> row.internId().equals(internId)).findFirst();
		}
		assertTrue(found.isPresent(), "expected the listing endpoint to include " + internId);
		assertEquals("Ada", found.get().firstName());
	}

	@Test
	void pausingTheListenerDelaysProcessingWithoutLosingTheMessage() throws InterruptedException, IOException {
		String internId = "INT-ADMIN-PAUSE-" + UUID.randomUUID();
		String recordId = UUID.randomUUID().toString();
		String json = envelopeJson(recordId, internId, "Grace", "Hopper",
				"grace@example.com", "Yale", "Compilers", "Howard");

		AdminController.AdminStatusResponse paused =
				restTemplate.postForObject("/admin/pause", null, AdminController.AdminStatusResponse.class);
		assertTrue(paused.paused(), "expected the listener to report paused after /admin/pause");

		try {
			publish(internId, json);

			Thread.sleep(2000);
			Path csvPath = tempDir.resolve("interns.csv");
			boolean processedWhilePaused = Files.exists(csvPath)
					&& Files.readAllLines(csvPath).stream().anyMatch(line -> line.contains(internId));
			assertFalse(processedWhilePaused, "the message must not be processed while the listener is paused");
		} finally {
			AdminController.AdminStatusResponse resumed =
					restTemplate.postForObject("/admin/resume", null, AdminController.AdminStatusResponse.class);
			assertFalse(resumed.paused(), "expected the listener to report running after /admin/resume");
		}

		boolean found = false;
		Path csvPath = tempDir.resolve("interns.csv");
		for (int i = 0; i < 20 && !found; i++) {
			Thread.sleep(500);
			found = Files.exists(csvPath) && Files.readAllLines(csvPath).stream().anyMatch(line -> line.contains(internId));
		}
		assertTrue(found, "expected the message to be processed once resumed -- it must not be lost");
	}
}
