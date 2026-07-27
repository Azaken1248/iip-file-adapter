package com.iip.fileadapter.reliability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes non-retriable / retry-exhausted failures to iip.dlq as a
 * DlqEnvelope (docs/03-data-model.md §3), preserving the original message
 * rather than discarding it -- this is what turns a poison message into a
 * quarantine instead of a pipeline stall (Architecture §6).
 */
@Component
public class DlqPublisher {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String dlqTopic;
	private final FailureClassifier classifier;

	public DlqPublisher(
			KafkaTemplate<String, String> kafkaTemplate,
			ObjectMapper objectMapper,
			@Value("${iip.topics.intern-dlq}") String dlqTopic,
			FailureClassifier classifier) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.dlqTopic = dlqTopic;
		this.classifier = classifier;
	}

	public void publish(ConsumerRecord<String, String> record, Throwable error, int attemptCount) {
		DlqEnvelope envelope = new DlqEnvelope(
				record.topic(),
				record.partition(),
				record.offset(),
				record.key(),
				record.value(),
				errorType(error),
				rootMessage(error),
				"file-adapter",
				attemptCount,
				Instant.now(),
				false,
				null);

		try {
			String json = objectMapper.writeValueAsString(envelope);
			kafkaTemplate.send(dlqTopic, record.key(), json).get(10, TimeUnit.SECONDS);
		} catch (JsonProcessingException | ExecutionException | TimeoutException e) {
			throw new IllegalStateException("failed to publish to DLQ topic " + dlqTopic, e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted publishing to DLQ topic " + dlqTopic, e);
		}
	}

	// Package-private (not private) so focused unit tests can exercise the
	// label decision directly without needing a real Kafka broker --
	// publish()'s own network behavior is already covered by the
	// Testcontainers-based DlqTest.
	String errorType(Throwable error) {
		// A RETRIABLE classification only reaches here once
		// BoundedRetryExecutor has exhausted every attempt against it --
		// that's the one case "RETRY_EXHAUSTED" actually describes. A
		// NON_RETRIABLE classification was never retried at all (it fails
		// on the very first attempt), so labeling it RETRY_EXHAUSTED would
		// misdiagnose a fundamentally bad message as a target that kept
		// failing when it never got the chance to.
		if (classifier.classify(error) == FailureClassification.RETRIABLE) {
			return "RETRY_EXHAUSTED";
		}
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (current instanceof JsonProcessingException) {
				return "SCHEMA_VIOLATION";
			}
		}
		return "UNCLASSIFIED_FAILURE";
	}

	private String rootMessage(Throwable error) {
		Throwable root = error;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		return root.getMessage();
	}
}
