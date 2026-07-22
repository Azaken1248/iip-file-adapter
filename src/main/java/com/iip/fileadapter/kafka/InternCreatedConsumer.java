package com.iip.fileadapter.kafka;

import com.iip.fileadapter.csv.CsvInternWriter;
import com.iip.fileadapter.dedup.DedupStore;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InternCreatedConsumer {

	private final DedupStore dedupStore;
	private final CsvInternWriter csvInternWriter;

	public InternCreatedConsumer(DedupStore dedupStore, CsvInternWriter csvInternWriter) {
		this.dedupStore = dedupStore;
		this.csvInternWriter = csvInternWriter;
	}

	@KafkaListener(topics = "${iip.topics.intern-created}")
	public void onInternCreated(InternCreatedEvent event) {
		if (dedupStore.isProcessed(event.recordId())) {
			return;
		}
		csvInternWriter.append(event);
		dedupStore.markProcessed(event.recordId());
	}
}
