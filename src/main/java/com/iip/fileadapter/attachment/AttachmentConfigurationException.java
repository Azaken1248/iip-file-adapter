package com.iip.fileadapter.attachment;

/**
 * The attachment says to write somewhere it cannot describe (Phase 5.7):
 * shaped mode with no table, no column mapping, or a name that is not a plain
 * SQL identifier.
 *
 * <p>Its own type because its own audience: nothing is wrong with the message
 * and nothing is wrong with this service. The record is quarantined so the
 * mistake is visible, and the fix is one edit in the control plane -- which is
 * a very different instruction from "look at the payload" or "wait for the
 * target to come back".
 *
 * <p>Non-retriable: an attachment does not change between two attempts
 * milliseconds apart, so retrying repeats the same complaint.
 */
public class AttachmentConfigurationException extends RuntimeException {

	public AttachmentConfigurationException(String message) {
		super(message);
	}
}
