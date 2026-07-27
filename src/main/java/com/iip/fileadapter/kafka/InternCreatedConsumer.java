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
	// groupId is set explicitly and separately: @KafkaListener's id doubles
	// as the Kafka consumer group id by default (idIsGroup), and this id
	// string isn't unique across services -- db-adapter uses the exact
	// same LISTENER_ID for its own listener. Without an explicit groupId
	// here, both adapters would silently join the *same* Kafka consumer
	// group and split interns.created's partitions between them instead of
	// each independently seeing every message, breaking the fan-out
	// guarantee (caught via a real docker-compose run, not the test suite,
	// since each adapter's Testcontainers tests use an isolated Kafka
	// broker with only one adapter ever connected to it).
	public static final String LISTENER_ID = "internCreatedListener";

	@KafkaListener(
			id = LISTENER_ID,
			groupId = "${spring.kafka.consumer.group-id}",
			topics = "${iip.topics.intern-created}")
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

	// The dedup key is recordId -- an envelope field, unique per event and
	// universal across every contract. Nothing in this method reads the
	// payload, which is precisely why the idempotency guarantee survived
	// the envelope split untouched.
	private void process(String json) {
		CanonicalEnvelope envelope = deserialize(json);
		if (dedupStore.isProcessed(envelope.recordId())) {
			return;
		}
		csvInternWriter.append(envelope);
		dedupStore.markProcessed(envelope.recordId());
	}

	private CanonicalEnvelope deserialize(String json) {
		try {
			return objectMapper.readValue(json, CanonicalEnvelope.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
