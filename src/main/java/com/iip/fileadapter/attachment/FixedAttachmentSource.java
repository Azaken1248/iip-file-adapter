package com.iip.fileadapter.attachment;

import java.util.List;
import java.util.Map;

/**
 * One attachment, from this deployment's own configuration (Phase 6.2).
 *
 * <p>A placeholder with a deliberately short life. Phase 6.2's job was to give
 * both adapters the same shape, and the shape has a contract filter in it -- so
 * this adapter needs something to filter <em>by</em>, and until Phase 6.3 it
 * has no attachment read path to get it from. This supplies exactly what the
 * service already believed: one contract, named in {@code application.yml},
 * which is what "{@code iip.topics.intern-created}" has meant since Release 1.
 *
 * <p>The result is that Phase 6.2 changes no behaviour here, which is what a
 * refactor should do, while making the missing piece obvious: the filter is
 * real, it is just being told the answer instead of asking. Phase 6.3 replaces
 * this class with the registry-backed source the db-adapter already uses, and
 * nothing else in the pipeline changes.
 */
public class FixedAttachmentSource implements AttachmentSource {

	private final String contractId;

	public FixedAttachmentSource(String contractId) {
		this.contractId = contractId;
	}

	@Override
	public List<Attachment> loadAll() {
		return List.of(new Attachment("configured", contractId, Map.of()));
	}

	@Override
	public String describe() {
		return "this deployment's configuration (contract '" + contractId + "')";
	}
}
