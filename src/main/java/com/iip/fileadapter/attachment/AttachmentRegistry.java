package com.iip.fileadapter.attachment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Which contracts this adapter is attached to, and how each is configured
 * (Phase 5.2).
 *
 * <p>Loaded once at construction and re-read on an interval, so attaching a
 * target through the control plane takes effect without a redeploy -- the whole
 * of [UC-14](02-use-cases.md)'s "no redeploy" as it applies to the data plane.
 * The cached map is replaced wholesale rather than mutated, so a message being
 * handled mid-refresh sees either the entire old set or the entire new one.
 *
 * <p><strong>Startup is fail-fast, and that asymmetry with refresh is the
 * important decision here.</strong> Phase 5.3 skips-and-commits any record
 * whose contract is not attached, which means an empty attachment set is
 * indistinguishable, at runtime, from "discard everything on this topic and
 * advance the offset past it". If an unreachable registry at boot left this
 * empty, the adapter would come up healthy and silently consume the backlog to
 * nothing. So construction propagates the failure and the container dies
 * noisily, which is recoverable; the alternative is not.
 *
 * <p>A refresh failure is the opposite case and is swallowed: the last
 * known-good set stays in place, exactly as the source-service does with
 * contracts. A control-plane outage must not take the data plane with it.
 *
 * <p>Zero attachments from a registry that <em>answered</em> is a normal
 * steady state -- a freshly-deployed adapter type nobody has attached anything
 * to yet -- and is not an error. It is logged loudly all the same, because the
 * observable behaviour (every record skipped) looks identical to a bug.
 */
public class AttachmentRegistry {

	private static final Logger log = LoggerFactory.getLogger(AttachmentRegistry.class);

	private final AttachmentSource source;

	private volatile Map<String, Attachment> byContractId;

	public AttachmentRegistry(AttachmentSource source) {
		this.source = source;
		log.info("Loading adapter attachments from {}", source.describe());
		this.byContractId = index(source.loadAll());
		logCurrentState();
	}

	@Scheduled(
			initialDelayString = "${iip.attachments.refresh-interval-ms:30000}",
			fixedDelayString = "${iip.attachments.refresh-interval-ms:30000}")
	public void refresh() {
		Map<String, Attachment> previous = byContractId;
		Map<String, Attachment> refreshed;

		try {
			refreshed = index(source.loadAll());
		}
		catch (RuntimeException e) {
			log.warn("Attachment refresh from {} failed; continuing with the {} attachment(s) already loaded: {}",
					source.describe(), previous.size(), e.getMessage());
			return;
		}

		this.byContractId = refreshed;
		logWhatChanged(previous, refreshed);
	}

	/**
	 * The question Phase 5.3's filter asks of every message.
	 */
	public boolean isAttached(String contractId) {
		return byContractId.containsKey(contractId);
	}

	public Optional<Attachment> find(String contractId) {
		return Optional.ofNullable(byContractId.get(contractId));
	}

	public Collection<Attachment> all() {
		return byContractId.values();
	}

	/**
	 * Keyed by contract, which quietly asserts one postgres attachment per
	 * contract. That is not a limitation of the table -- {@code UNIQUE
	 * (contract_id, adapter_type, config)} permits two postgres targets for one
	 * contract -- but nothing in Release 5 has a use for a second, and silently
	 * keeping whichever the registry happened to list last would be a genuinely
	 * horrible bug to track down. So it is named, loudly, and the first one
	 * wins deterministically.
	 */
	private static Map<String, Attachment> index(Iterable<Attachment> loaded) {
		Map<String, Attachment> byId = new LinkedHashMap<>();
		for (Attachment attachment : loaded) {
			Attachment previous = byId.putIfAbsent(attachment.contractId(), attachment);
			if (previous != null) {
				log.warn("Contract '{}' has more than one enabled postgres attachment ({} and {}); "
								+ "using {} and ignoring the rest",
						attachment.contractId(), previous.attachmentId(), attachment.attachmentId(),
						previous.attachmentId());
			}
		}
		return Map.copyOf(byId);
	}

	private void logCurrentState() {
		if (byContractId.isEmpty()) {
			log.warn("No contracts are attached to this adapter. Every record consumed will be skipped "
					+ "until an attachment is registered ({}).", source.describe());
			return;
		}
		log.info("Attached to {} contract(s): {}", byContractId.size(), byContractId.keySet());
	}

	private static void logWhatChanged(Map<String, Attachment> previous, Map<String, Attachment> current) {
		for (Attachment attachment : current.values()) {
			Attachment before = previous.get(attachment.contractId());
			if (before == null) {
				log.info("Contract '{}' is now attached to this adapter (attachment {})",
						attachment.contractId(), attachment.attachmentId());
			}
			else if (!before.config().equals(attachment.config())) {
				log.info("Contract '{}' attachment config changed: {} -> {}",
						attachment.contractId(), before.config(), attachment.config());
			}
		}
		for (String contractId : previous.keySet()) {
			if (!current.containsKey(contractId)) {
				// Detached or disabled -- indistinguishable from here, and the
				// consequence is the same either way: records for this contract
				// start being skipped, which an operator will want in the log
				// next to the time it started happening.
				log.warn("Contract '{}' is no longer attached; its records will now be skipped", contractId);
			}
		}
	}
}
