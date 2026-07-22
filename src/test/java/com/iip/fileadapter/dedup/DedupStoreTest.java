package com.iip.fileadapter.dedup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DedupStoreTest {

	@TempDir
	Path tempDir;

	@Test
	void anUnmarkedRecordIdIsNotProcessed() {
		DedupStore store = new DedupStore(tempDir.resolve("dedup"));
		assertFalse(store.isProcessed(UUID.randomUUID()));
	}

	@Test
	void aMarkedRecordIdIsProcessed() {
		DedupStore store = new DedupStore(tempDir.resolve("dedup"));
		UUID recordId = UUID.randomUUID();

		store.markProcessed(recordId);

		assertTrue(store.isProcessed(recordId));
	}

	@Test
	void markingSurvivesAFreshStoreInstancePointedAtTheSameFile() {
		Path storePath = tempDir.resolve("dedup");
		UUID recordId = UUID.randomUUID();

		new DedupStore(storePath).markProcessed(recordId);

		DedupStore reloaded = new DedupStore(storePath);
		assertTrue(reloaded.isProcessed(recordId), "expected the mark to survive a restart (same backing file)");
	}
}
