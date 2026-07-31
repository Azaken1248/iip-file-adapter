package com.iip.fileadapter.acceptance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * <strong>The acceptance suite every adapter type must pass</strong> (Phase
 * 6.10).
 *
 * <p>[Architecture §6](01-architecture.md) says its pipeline diagram is "the
 * acceptance checklist for any new adapter", and that an adapter which skips
 * the idempotency gate or the failure classifier "silently breaks the
 * platform's core promise". This is that checklist, executable.
 *
 * <p>It exists because of a decision made in Phase 6.2. Every adapter carries
 * its own copy of the pipeline rather than depending on a shared jar -- this
 * platform has no artifact repository, and [UC-9](02-use-cases.md)'s promise is
 * that a new adapter type is a new service, not a new service plus a versioned
 * dependency on ours. The cost of that choice is drift, and drift in this
 * particular code is a reliability guarantee quietly lost in whichever adapter
 * nobody is looking at. The compatibility gate compares the pipeline classes
 * character by character; this checks the <em>behaviour</em>, which is the part
 * that actually matters and the part a clever refactor can break without
 * changing a shared file.
 *
 * <p>Every assertion here is about the envelope and nothing else, which is what
 * makes one suite serve a database, a file and an HTTP endpoint. A subclass
 * supplies four things: how to publish, how to count what landed, how to reach
 * the DLQ, and what "an attached contract" means for it.
 *
 * <p>Copied into each adapter's repository, like the pipeline it tests, and
 * kept identical by the same gate.
 */
public abstract class AdapterAcceptanceSuite {

	/** A contract this adapter is attached to. */
	protected abstract String attachedContractId();

	/** A contract it is not, for the filter check. */
	protected String unattachedContractId() {
		return "invoices";
	}

	/** The topic to publish onto. Routing is by contractId, not topic name. */
	protected abstract String topic();

	protected abstract String bootstrapServers();

	protected abstract String dlqTopic();

	/** A valid record for {@link #attachedContractId()} with this natural key. */
	protected abstract String validRecord(String recordId, String naturalKey);

	/** A record for {@link #unattachedContractId()}. */
	protected abstract String unattachedRecord(String recordId, String naturalKey);

	/**
	 * How many times this record reached the target. The whole suite rests on
	 * this being counted at the target rather than inferred from the adapter's
	 * own logs -- "we think we wrote it once" is not the guarantee.
	 */
	protected abstract long timesApplied(String naturalKey) throws Exception;

	/** Blocks until the record has landed, or gives up. */
	protected void awaitApplied(String naturalKey) throws Exception {
		for (int i = 0; i < 40; i++) {
			if (timesApplied(naturalKey) > 0) {
				return;
			}
			Thread.sleep(500);
		}
		fail("record '" + naturalKey + "' never reached the target");
	}

	// --- the checklist -------------------------------------------------------

	/**
	 * <strong>Idempotency.</strong> At-least-once delivery is the platform's
	 * transport guarantee, so every adapter has to turn it into
	 * effectively-once at its target -- by constraint, by a dedup store, or by
	 * whatever that target makes possible. An adapter that fails this looks
	 * perfect until a rebalance or a replay, and then quietly does everything
	 * twice.
	 */
	@Test
	void deliveringTheSameRecordTwiceAppliesItOnce() throws Exception {
		String naturalKey = "ACC-IDEMPOTENT-" + UUID.randomUUID();
		String record = validRecord(UUID.randomUUID().toString(), naturalKey);

		publish(topic(), naturalKey, record);
		publish(topic(), naturalKey, record);

		awaitApplied(naturalKey);
		// Long enough for a second application to have happened if it were
		// going to; stopping at the first success would pass an adapter that
		// applies everything twice.
		Thread.sleep(2000);

		assertEquals(1, timesApplied(naturalKey),
				"redelivery was applied more than once -- this adapter has no idempotency gate");
	}

	/**
	 * <strong>The contract filter.</strong> Every adapter sees every contract's
	 * records, so it must write only what it is attached to -- and, just as
	 * importantly, must not stall on the rest. A record it skips has to commit
	 * its offset, or the first unattached contract on the platform blocks the
	 * partition behind it forever.
	 */
	@Test
	void anUnattachedContractsRecordIsSkippedWithoutStallingThePartition() throws Exception {
		publish(topic(), "ACC-FILTER", unattachedRecord(UUID.randomUUID().toString(), "ACC-FILTER"));

		// Behind the skipped one, on the same topic. It cannot arrive unless the
		// skipped record's offset committed.
		String naturalKey = "ACC-FILTER-AFTER-" + UUID.randomUUID();
		publish(topic(), naturalKey, validRecord(UUID.randomUUID().toString(), naturalKey));

		awaitApplied(naturalKey);
	}

	/**
	 * <strong>The DLQ.</strong> A message this adapter cannot handle must be
	 * quarantined rather than retried forever or dropped -- and the partition
	 * must keep moving. This is the difference between a poison message costing
	 * one entry in a queue and costing the whole pipeline.
	 */
	@Test
	void aMalformedRecordIsQuarantinedAndThePartitionKeepsMoving() throws Exception {
		String poison = "{\"contractId\": \"" + attachedContractId() + "\", not json";
		publish(topic(), "ACC-DLQ", poison);

		String naturalKey = "ACC-DLQ-AFTER-" + UUID.randomUUID();
		publish(topic(), naturalKey, validRecord(UUID.randomUUID().toString(), naturalKey));

		// The liveness half first: if this never lands, the adapter stalled on
		// the poison message and nothing below matters.
		awaitApplied(naturalKey);

		assertTrue(drainDlq().stream().anyMatch(entry -> entry.contains("ACC-DLQ")),
				"a message this adapter could not handle was neither applied nor quarantined -- it was lost");
	}

	// --- helpers -------------------------------------------------------------

	protected List<String> drainDlq() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "acceptance-dlq-" + UUID.randomUUID());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(List.of(dlqTopic()));
			List<String> entries = new ArrayList<>();
			for (int i = 0; i < 5; i++) {
				ConsumerRecords<String, String> polled = consumer.poll(Duration.ofSeconds(5));
				if (polled.isEmpty() && i > 0) {
					break;
				}
				polled.forEach(record -> entries.add(record.value()));
			}
			return entries;
		}
	}

	protected void publish(String topic, String key, String json) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
			producer.send(new ProducerRecord<>(topic, key, json)).get();
		} catch (Exception e) {
			fail("failed to publish test message: " + e.getMessage());
		}
	}
}
