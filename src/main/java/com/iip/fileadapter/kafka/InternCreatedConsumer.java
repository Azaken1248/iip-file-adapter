package com.iip.fileadapter.kafka;

import com.iip.fileadapter.pipeline.RecordPipeline;
import com.iip.fileadapter.reliability.BoundedRetryExecutor;
import com.iip.fileadapter.reliability.DlqPublisher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Delivery: takes records off the topic and makes sure each one is either
 * applied, quarantined, or skipped -- and that the offset moves either way.
 *
 * <p>Everything about <em>what</em> a record means now lives in
 * {@link RecordPipeline}, which after Phase 6.2 reads identically to the
 * db-adapter's. What is left here is the part that is genuinely about Kafka:
 * the subscription, the retry budget, and the guarantee that a message this
 * adapter cannot handle does not stall the partition behind it.
 *
 * <p>Still bound to one named topic, unlike the db-adapter's pattern
 * subscription. That is the visible remainder of this adapter not yet having an
 * attachment read path, and it is Phase 6.3's subject rather than an oversight:
 * subscribing to every contract's stream would be pointless while the only
 * contract it can be attached to is the one in its own configuration.
 */
@Component
public class InternCreatedConsumer {

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

	private final RecordPipeline pipeline;
	private final BoundedRetryExecutor retryExecutor;
	private final DlqPublisher dlqPublisher;

	public InternCreatedConsumer(
			RecordPipeline pipeline,
			BoundedRetryExecutor retryExecutor,
			DlqPublisher dlqPublisher) {

		this.pipeline = pipeline;
		this.retryExecutor = retryExecutor;
		this.dlqPublisher = dlqPublisher;
	}

	@KafkaListener(
			id = LISTENER_ID,
			groupId = "${spring.kafka.consumer.group-id}",
			topics = "${iip.topics.intern-created}")
	public void onInternCreated(ConsumerRecord<String, String> record) {
		AtomicInteger attempts = new AtomicInteger(0);
		try {
			retryExecutor.executeWithRetry(() -> {
				attempts.incrementAndGet();
				pipeline.apply(record.value());
			});
		} catch (RuntimeException e) {
			// Handled, not lost: quarantine to the DLQ, then let the
			// listener return normally so the offset commits and the
			// pipeline keeps moving instead of stalling on this message.
			dlqPublisher.publish(record, e, attempts.get());
		}
	}
}
