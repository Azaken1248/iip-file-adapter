package com.iip.fileadapter.acceptance;

import com.iip.fileadapter.TestcontainersConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Files;
import java.nio.file.Path;

import static com.iip.fileadapter.EnvelopeJsonFixture.envelopeJson;

/**
 * The csv adapter against the platform's acceptance checklist (Phase 6.10).
 *
 * <p>Counted at the target, which here means counting lines in the file. That
 * is the honest measure for this adapter: its idempotency guarantee is a dedup
 * store rather than a constraint, and a store that says "already written" while
 * the file has two lines would be worth nothing.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class CsvAdapterAcceptanceTest extends AdapterAcceptanceSuite {

	@Autowired
	private KafkaContainer kafkaContainer;

	@Value("${iip.file.output-path}")
	private String outputPath;

	@Override
	protected String attachedContractId() {
		return "interns";
	}

	@Override
	protected String topic() {
		return "interns.created";
	}

	@Override
	protected String bootstrapServers() {
		return kafkaContainer.getBootstrapServers();
	}

	@Override
	protected String dlqTopic() {
		return "iip.dlq";
	}

	@Override
	protected String validRecord(String recordId, String naturalKey) {
		return envelopeJson(recordId, naturalKey, "Ada", "Lovelace",
				"ada@example.com", "MIT", "Platform", "Sam");
	}

	@Override
	protected String unattachedRecord(String recordId, String naturalKey) {
		return envelopeJson(recordId, unattachedContractId(), "invoice.created", naturalKey,
				"{\"invoiceNumber\": \"INV-1\"}");
	}

	@Override
	protected long timesApplied(String naturalKey) throws Exception {
		Path file = Path.of(outputPath);
		if (!Files.exists(file)) {
			return 0;
		}
		try (var lines = Files.lines(file)) {
			return lines.filter(line -> line.contains(naturalKey)).count();
		}
	}
}
