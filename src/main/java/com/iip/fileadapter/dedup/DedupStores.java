package com.iip.fileadapter.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One dedup store per contract (Phase 6.3).
 *
 * <p>Until now this adapter served one contract and had one store. Serving
 * several from one instance makes the split necessary rather than tidy: the
 * store is what stands in for the idempotency constraint a file cannot provide,
 * and a shared one would let two contracts interfere in a way that is very hard
 * to see.
 *
 * <p>The concrete failure a shared store allows: {@code recordId} is a UUID and
 * collisions are not the concern. Replay is. Rewinding one contract's consumer
 * offset to re-deliver its records -- a legitimate operation, and the reason
 * the platform keeps topic retention -- means clearing its dedup state, and
 * with one store that means clearing every contract's, so the next redelivery
 * of anything duplicates its line. Per-contract stores make "replay this
 * contract" an operation that touches only that contract, which is what an
 * operator would already assume.
 */
public class DedupStores {

	private static final Logger log = LoggerFactory.getLogger(DedupStores.class);

	private final Path baseDirectory;
	private final Map<String, DedupStore> byContractId = new ConcurrentHashMap<>();

	/**
	 * Record ids the single-store version of this adapter had already written,
	 * or empty on a deployment that never ran one.
	 */
	private final List<String> inherited;

	public DedupStores(Path baseDirectory) {
		this.baseDirectory = baseDirectory;
		this.inherited = adoptSingleStoreIfPresent(baseDirectory);
	}

	public DedupStore forContract(String contractId) {
		// computeIfAbsent so two partitions of the same contract arriving at
		// once cannot build two stores over one file, which would give each a
		// half-view of what had been written.
		return byContractId.computeIfAbsent(contractId, this::create);
	}

	private DedupStore create(String contractId) {
		Path store = baseDirectory.resolve(sanitize(contractId) + ".dedup");
		seedFromInherited(store);
		return new DedupStore(store);
	}

	/**
	 * Takes over the file the previous single-store version wrote.
	 *
	 * <p>Phase 6.3 changed what {@code iip.file.dedup-store-path} <em>means</em>
	 * -- from the store file to the directory holding one store per contract --
	 * and an existing deployment has a regular file sitting exactly where this
	 * version wants a directory. Left alone that is not a clean failure: the
	 * adapter starts, and then every write fails with "Not a directory" and
	 * every record goes to the DLQ. Config that changes meaning underneath a
	 * running deployment has to migrate itself or say so; silently breaking is
	 * the one option that is not available.
	 *
	 * <p>The old file's contents are kept rather than discarded. Those ids are
	 * the record of what has already been appended, and dropping them would
	 * mean a replay silently duplicating every line written before the upgrade.
	 */
	private static List<String> adoptSingleStoreIfPresent(Path baseDirectory) {
		try {
			if (!Files.isRegularFile(baseDirectory)) {
				Files.createDirectories(baseDirectory);
				return List.of();
			}

			List<String> entries = Files.readAllLines(baseDirectory);
			Path retired = baseDirectory.resolveSibling(baseDirectory.getFileName() + ".single-store");
			Files.move(baseDirectory, retired, StandardCopyOption.REPLACE_EXISTING);
			Files.createDirectories(baseDirectory);

			log.info("Migrated the single dedup store at {} to a directory of per-contract stores, carrying over "
					+ "{} already-written record(s). The original file is kept at {}.",
					baseDirectory, entries.size(), retired);
			return entries;
		} catch (IOException e) {
			throw new UncheckedIOException("could not prepare the dedup store directory " + baseDirectory, e);
		}
	}

	/**
	 * Every contract's first store inherits the pre-upgrade ids.
	 *
	 * <p>Copying one contract's history into another's store sounds wrong and
	 * is harmless: a {@code recordId} is unique per event across the whole
	 * platform, so an id from another contract is one this store will never be
	 * asked about. The alternative -- working out which contract each id
	 * belonged to -- is not possible from the file, which records ids and
	 * nothing else.
	 */
	private void seedFromInherited(Path store) {
		if (inherited.isEmpty() || Files.exists(store)) {
			return;
		}
		try {
			Files.write(store, inherited);
		} catch (IOException e) {
			throw new UncheckedIOException("could not seed the dedup store " + store, e);
		}
	}

	/**
	 * A contract id is whatever someone typed into the control plane, and from
	 * Phase 6.7 that is a browser text box. It has to become a filename, so
	 * anything that is not safe in one is replaced rather than refused --
	 * {@code purchase/orders} must not be able to write outside the store
	 * directory.
	 */
	private static String sanitize(String contractId) {
		return contractId.replaceAll("[^A-Za-z0-9_.-]", "_");
	}
}
