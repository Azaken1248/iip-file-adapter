package com.iip.fileadapter.attachment;

/**
 * The Contract Registry could not be reached or did not answer usefully.
 *
 * <p>Fatal at startup, survivable on refresh -- see {@link AttachmentRegistry}
 * for why those two differ.
 */
public class AttachmentRegistryUnavailableException extends RuntimeException {

	public AttachmentRegistryUnavailableException(String baseUrl, Throwable cause) {
		super("the Contract Registry at " + baseUrl + " could not be reached: " + cause.getMessage(), cause);
	}
}
