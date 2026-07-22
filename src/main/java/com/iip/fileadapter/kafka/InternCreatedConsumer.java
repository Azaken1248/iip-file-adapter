package com.iip.fileadapter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.csv.CsvInternWriter;
import com.iip.fileadapter.dedup.DedupStore;
import com.iip.fileadapter.reliability.BoundedRetryExecutor;
import com.iip.fileadapter.reliability.DlqPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Deserialization happens by hand here (not via a Kafka JsonDeserializer)
 * so a malformed payload is just another failure the retry+classify+DLQ
 * pipeline handles uniformly -- Architecture §6's DESER and WRITE steps
 * are both "the action" as far as BoundedRetryExecutor is concerned.
 */
@Component
public class InternCreatedConsumer {

	private final DedupStore dedupStore;
	private final CsvInternWriter csvInternWriter;
	private final ObjectMapper objectMapper;
	private final BoundedRetryExecutor retryExecutor;
	private final DlqPublisher dlqPublisher;

	public InternCreatedConsumer(
			DedupStore dedupStore,
			CsvInternWriter csvInternWriter,
			ObjectMapper objectMapper,
			BoundedRetryExecutor retryExecutor,
			DlqPublisher dlqPublisher) {
		this.dedupStore = dedupStore;
		this.csvInternWriter = csvInternWriter;
		this.objectMapper = objectMapper;
		this.retryExecutor = retryExecutor;
		this.dlqPublisher = dlqPublisher;
	}

	// The explicit id is what AdminController looks up via
	// KafkaListenerEndpointRegistry to pause/resume this specific listener.
	public static final String LISTENER_ID = "internCreatedListener";

	@KafkaListener(id = LISTENER_ID, topics = "${iip.topics.intern-created}")
	public void onInternCreated(ConsumerRecord<String, String> record) {
		AtomicInteger attempts = new AtomicInteger(0);
		try {
			retryExecutor.executeWithRetry(() -> {
				attempts.incrementAndGet();
				process(record.value());
			});
		} catch (RuntimeException e) {
			// Handled, not lost: quarantine to the DLQ, then let the
			// listener return normally so the offset commits and the
			// pipeline keeps moving instead of stalling on this message.
			dlqPublisher.publish(record, e, attempts.get());
		}
	}

	private void process(String json) {
		InternCreatedEvent event = deserialize(json);
		if (dedupStore.isProcessed(event.recordId())) {
			return;
		}
		csvInternWriter.append(event);
		dedupStore.markProcessed(event.recordId());
	}

	private InternCreatedEvent deserialize(String json) {
		try {
			return objectMapper.readValue(json, InternCreatedEvent.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
