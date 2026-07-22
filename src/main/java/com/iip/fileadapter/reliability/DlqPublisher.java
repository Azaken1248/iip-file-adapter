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
 * Publishes non-retriable / retry-exhausted failures to intern.dlq as a
 * DlqEnvelope (docs/03-data-model.md §3), preserving the original message
 * rather than discarding it -- this is what turns a poison message into a
 * quarantine instead of a pipeline stall (Architecture §6).
 */
@Component
public class DlqPublisher {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String dlqTopic;

	public DlqPublisher(
			KafkaTemplate<String, String> kafkaTemplate,
			ObjectMapper objectMapper,
			@Value("${iip.topics.intern-dlq}") String dlqTopic) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.dlqTopic = dlqTopic;
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

	private String errorType(Throwable error) {
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (current instanceof JsonProcessingException) {
				return "SCHEMA_VIOLATION";
			}
		}
		return "RETRY_EXHAUSTED";
	}

	private String rootMessage(Throwable error) {
		Throwable root = error;
		while (root.getCause() != null) {
			root = root.getCause();
		}
		return root.getMessage();
	}
}
