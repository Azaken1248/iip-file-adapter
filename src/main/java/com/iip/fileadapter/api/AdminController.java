package com.iip.fileadapter.api;

import com.iip.fileadapter.csv.CsvInternReader;
import com.iip.fileadapter.csv.InternRow;
import com.iip.fileadapter.kafka.InternCreatedConsumer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Mirrors db-adapter's AdminController: pauses/resumes this adapter's own
 * Kafka listener rather than the target container itself, since a
 * container can't pause itself, and pausing the listener proves the
 * identical zero-data-loss guarantee -- while paused the consumer group's
 * offset doesn't advance, so nothing published during the pause is lost.
 */
@RestController
public class AdminController {

	private static final int CONFIRM_ATTEMPTS = 20;
	private static final long CONFIRM_POLL_MS = 100;

	private final CsvInternReader csvInternReader;
	private final KafkaListenerEndpointRegistry registry;

	public AdminController(CsvInternReader csvInternReader, KafkaListenerEndpointRegistry registry) {
		this.csvInternReader = csvInternReader;
		this.registry = registry;
	}

	@GetMapping("/interns")
	public List<InternRow> listInterns() {
		return csvInternReader.readAll();
	}

	@PostMapping("/admin/pause")
	public AdminStatusResponse pause() {
		container().pause();
		waitUntil(true);
		return status();
	}

	@PostMapping("/admin/resume")
	public AdminStatusResponse resume() {
		container().resume();
		waitUntil(false);
		return status();
	}

	@GetMapping("/admin/status")
	public AdminStatusResponse status() {
		return new AdminStatusResponse(container().isContainerPaused());
	}

	private void waitUntil(boolean pausedState) {
		for (int i = 0; i < CONFIRM_ATTEMPTS && container().isContainerPaused() != pausedState; i++) {
			try {
				Thread.sleep(CONFIRM_POLL_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}

	private MessageListenerContainer container() {
		return registry.getListenerContainer(InternCreatedConsumer.LISTENER_ID);
	}

	public record AdminStatusResponse(boolean paused) {
	}
}
