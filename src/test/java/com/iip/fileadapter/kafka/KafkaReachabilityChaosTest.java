package com.iip.fileadapter.kafka;

import com.iip.fileadapter.TestcontainersConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Phase 2.3: the same chaos-suite formalization as Phase 2.2, but for a
 * broker disruption rather than a target disruption -- Kafka itself
 * becoming unreachable, which is a different failure surface than
 * anything BoundedRetryExecutor/FailureClassifier touch (no message ever
 * reaches onInternCreated() while the broker is down, so this is entirely
 * about whether the consumer container reconnects and resumes on its own
 * once the broker comes back, not about the retry/DLQ pipeline).
 *
 * Lives in file-adapter specifically (per Phase 2.3's own scope), using
 * Docker pause/unpause on file-adapter's own isolated Kafka container --
 * the same proven technique as Phases 1.16/2.2, for the same reason
 * (stop/restart of a single Testcontainers broker isn't reliable; pausing
 * freezes the process without tearing down the network binding it's
 * already bound to).
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class KafkaReachabilityChaosTest {

	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.resolve("interns.csv").toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
	}

	@Autowired
	private KafkaContainer kafkaContainer;

	private void publishBlocking(String key, String json) {
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

	private String internJson(String recordId, String internId, String firstName) {
		return envelopeJson(recordId, internId, firstName, "Test",
				firstName.toLowerCase() + "@example.com", "MIT", "Chaos Engineering", "Sam");
	}

	@Test
	void messagesAcrossABrokerOutageAllLandExactlyOnceOnceKafkaRecovers() throws Exception {
		String suffix = UUID.randomUUID().toString();
		String internBefore = "INT-KAFKA-CHAOS-BEFORE-" + suffix;
		String internDuring = "INT-KAFKA-CHAOS-DURING-" + suffix;

		String containerId = kafkaContainer.getContainerId();
		var dockerClient = DockerClientFactory.instance().client();

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			// Published while Kafka is still reachable, using an already-
			// live producer -- this is what lets the *during* send below
			// stay non-blocking: a brand-new producer created after the
			// broker is already paused can't fetch its initial metadata
			// and send() blocks synchronously waiting for it (a real
			// deadlock against this test's own pause/unpause structure,
			// hit for real on the first version of this test); a producer
			// that already has cached metadata from a prior send just
			// queues the record and lets its background I/O thread retry
			// the actual network send once the broker's reachable again.
			producer.send(new ProducerRecord<>("intern.created", internBefore,
					internJson(UUID.randomUUID().toString(), internBefore, "Before"))).get();

			dockerClient.pauseContainerCmd(containerId).exec();
			try {
				producer.send(new ProducerRecord<>("intern.created", internDuring,
						internJson(UUID.randomUUID().toString(), internDuring, "During")));
				Thread.sleep(3000);
			} finally {
				dockerClient.unpauseContainerCmd(containerId).exec();
			}
			producer.flush();
		}

		Path csvPath = tempDir.resolve("interns.csv");
		for (String internId : List.of(internBefore, internDuring)) {
			boolean found = false;
			for (int i = 0; i < 40 && !found; i++) {
				Thread.sleep(500);
				if (Files.exists(csvPath)) {
					found = Files.readAllLines(csvPath).stream().anyMatch(line -> line.contains(internId));
				}
			}
			assertTrue(found, "expected " + internId + " to land once Kafka recovered -- it must not be lost");
		}

		long durationOccurrences = Files.readAllLines(csvPath).stream()
				.filter(line -> line.contains(internDuring))
				.count();
		assertEquals(1, durationOccurrences, "a message spanning the outage must land exactly once, not be duplicated");
	}

	@Test
	void repeatedBrokerFlappingStillDeliversExactlyOnce() throws IOException, InterruptedException {
		String internId = "INT-KAFKA-CHAOS-FLAP-" + UUID.randomUUID();

		String containerId = kafkaContainer.getContainerId();
		var dockerClient = DockerClientFactory.instance().client();

		publishBlocking(internId, internJson(UUID.randomUUID().toString(), internId, "Flap"));

		for (int flap = 0; flap < 2; flap++) {
			dockerClient.pauseContainerCmd(containerId).exec();
			Thread.sleep(1500);
			dockerClient.unpauseContainerCmd(containerId).exec();
			Thread.sleep(500);
		}

		Path csvPath = tempDir.resolve("interns.csv");
		boolean found = false;
		for (int i = 0; i < 40 && !found; i++) {
			Thread.sleep(500);
			if (Files.exists(csvPath)) {
				found = Files.readAllLines(csvPath).stream().anyMatch(line -> line.contains(internId));
			}
		}
		assertTrue(found, "expected the message to eventually land despite repeated broker disruption");

		long occurrences = Files.readAllLines(csvPath).stream().filter(line -> line.contains(internId)).count();
		assertEquals(1, occurrences, "repeated broker disruption must still never duplicate a delivered message");
	}
}
