package com.iip.fileadapter.attachment;

import java.util.List;

/**
 * Where this adapter's attachments come from (Phase 5.2).
 *
 * <p>An interface for the same reason the source-service's {@code
 * ContractSource} is one: the tests need a source that does not require a
 * running Contract Registry, and the alternative -- a boolean flag inside the
 * HTTP client -- puts test-only branches in the class that talks to production.
 */
public interface AttachmentSource {

	/** Every enabled attachment for this adapter, newest state each call. */
	List<Attachment> loadAll();

	/** For logs: where these came from, in words an operator can act on. */
	String describe();
}
