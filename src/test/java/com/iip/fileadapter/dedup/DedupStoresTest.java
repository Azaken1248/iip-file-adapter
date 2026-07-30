package com.iip.fileadapter.dedup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 6.3 -- one dedup store per contract, and the upgrade that gets an
 * existing deployment there.
 */
class DedupStoresTest {

	@TempDir
	Path tempDir;

	@Test
	void twoContractsDoNotSeeEachOthersRecords() {
		DedupStores stores = new DedupStores(tempDir.resolve("dedup"));
		UUID recordId = UUID.randomUUID();

		stores.forContract("interns").markProcessed(recordId);

		assertTrue(stores.forContract("interns").isProcessed(recordId));
		// The point of the split. A shared store would answer true here, and
		// the second contract's record would be dropped as a duplicate of a
		// record it has nothing to do with.
		assertFalse(stores.forContract("purchase-orders").isProcessed(recordId),
				"one contract's dedup state suppressed another contract's record");
	}

	@Test
	void aContractIdThatIsNotAValidFilenameStillGetsAStore() {
		DedupStores stores = new DedupStores(tempDir.resolve("dedup"));
		UUID recordId = UUID.randomUUID();

		// It must not be able to escape the store directory either -- a
		// contract id is a browser text box from Phase 6.7 onwards.
		stores.forContract("../../etc/passwd").markProcessed(recordId);

		assertTrue(stores.forContract("../../etc/passwd").isProcessed(recordId));
		assertTrue(Files.exists(tempDir.resolve("dedup")), "the store directory was escaped");
	}

	/**
	 * The upgrade this phase forces on an existing deployment.
	 *
	 * <p>{@code iip.file.dedup-store-path} changed meaning -- from the store
	 * file to the directory holding one store per contract -- so a deployment
	 * that has been running since Release 1 has a regular file exactly where
	 * this version wants a directory. Without this, the adapter starts fine and
	 * then sends every record to the DLQ with "Not a directory", which is the
	 * worst of the available failures: late, uniform, and nothing to do with
	 * the record being processed.
	 */
	@Test
	void anExistingSingleStoreFileIsMigratedRatherThanCrashedInto() throws IOException {
		Path legacyPath = tempDir.resolve("dedup");
		UUID alreadyWritten = UUID.randomUUID();
		Files.write(legacyPath, List.of(alreadyWritten.toString()));

		DedupStores stores = new DedupStores(legacyPath);

		assertTrue(Files.isDirectory(legacyPath), "the store path should now be a directory");
		assertTrue(Files.isRegularFile(tempDir.resolve("dedup.single-store")),
				"the original file should be kept, not deleted");

		// And the history is carried over: a record already appended before the
		// upgrade must not be appended again on a replay.
		assertTrue(stores.forContract("interns").isProcessed(alreadyWritten),
				"the pre-upgrade dedup history was lost, so a replay would duplicate every line written before it");
	}

	@Test
	void aFreshDeploymentInheritsNothing() {
		DedupStores stores = new DedupStores(tempDir.resolve("dedup"));

		assertFalse(stores.forContract("interns").isProcessed(UUID.randomUUID()));
		assertEquals(0, tempDir.resolve("dedup").toFile().list().length,
				"a fresh deployment should not have a store until a contract needs one");
	}
}
