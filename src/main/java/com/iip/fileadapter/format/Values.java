package com.iip.fileadapter.format;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Reading a payload field as text, the same way in every format (Phase 6.4).
 */
final class Values {

	private Values() {
	}

	/**
	 * A payload field as text, or empty when the contract left it out.
	 *
	 * <p>Empty rather than the string "null", which is what a naive
	 * {@code String.valueOf} would produce and what a downstream spreadsheet
	 * would then show as a value. An absent optional field and an empty one are
	 * the same thing in a flat export, which has no way to say otherwise.
	 */
	static String text(JsonNode payload, String field) {
		JsonNode value = payload.path(field);
		if (value.isMissingNode() || value.isNull()) {
			return "";
		}
		return value.isValueNode() ? value.asText() : value.toString();
	}

	static JsonNode node(JsonNode payload, String field) {
		return payload.path(field);
	}
}
