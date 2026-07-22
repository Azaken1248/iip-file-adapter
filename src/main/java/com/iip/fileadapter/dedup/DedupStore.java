package com.iip.fileadapter.dedup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local seen-set of processed recordIds (docs/03-data-model.md §4.2) --
 * consulted before every CSV append and updated after every successful
 * write, since "append a line" isn't naturally idempotent on its own.
 * Backed by a flat file (one recordId per line) rather than an embedded
 * database: file-adapter runs as exactly one instance (Architecture
 * AD-6), so there's no concurrent-writer race to guard against, and a
 * flat file is the simplest thing that satisfies Release 1.
 */
public class DedupStore {

	private final Path storePath;
	private final Set<UUID> seen = ConcurrentHashMap.newKeySet();

	public DedupStore(Path storePath) {
		this.storePath = storePath;
		loadExisting();
	}

	private void loadExisting() {
		try {
			if (storePath.getParent() != null) {
				Files.createDirectories(storePath.getParent());
			}
			if (Files.exists(storePath)) {
				List<String> lines = Files.readAllLines(storePath);
				for (String line : lines) {
					if (!line.isBlank()) {
						seen.add(UUID.fromString(line.trim()));
					}
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public boolean isProcessed(UUID recordId) {
		return seen.contains(recordId);
	}

	public synchronized void markProcessed(UUID recordId) {
		if (seen.add(recordId)) {
			try {
				Files.writeString(storePath, recordId + System.lineSeparator(),
						StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}
}
