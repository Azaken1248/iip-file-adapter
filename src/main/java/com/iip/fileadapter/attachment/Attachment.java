package com.iip.fileadapter.attachment;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One row of {@code adapter_attachments} as this adapter sees it: "contract X
 * fans out to me, configured like this" (Phase 5.2).
 *
 * <p>{@code adapterType} is not a field. Everything this service ever sees is
 * {@code postgres} by construction -- it asks the registry for its own type and
 * nothing else -- so carrying the value around would invite code to branch on a
 * constant.
 *
 * <p>{@code config} stays an untyped map on purpose. What a valid postgres
 * attachment config looks like is still being discovered across Phases 5.4-5.7,
 * and the catalog that will declare it formally is Phase 6.1. Binding it to a
 * record now would fix the shape before two adapter types exist to generalize
 * over -- the premature-abstraction risk [AD-8](01-architecture.md) names.
 */
public record Attachment(String attachmentId, String contractId, Map<String, Object> config) {

	public Attachment {
		config = config == null ? Map.of() : Map.copyOf(config);
	}

	/**
	 * A config value, or a default when the attachment does not set one.
	 *
	 * <p>Absent and present-but-wrong-type are deliberately not distinguished
	 * here; Phase 6.1's catalog is where a config gets validated against a
	 * declared schema, and duplicating half of that check now would leave two
	 * places to fix when it lands.
	 */
	public String configString(String key, String fallback) {
		return config.get(key) instanceof String value ? value : fallback;
	}

	/**
	 * A nested config object -- a column mapping, for instance (Phase 5.7) --
	 * or an empty map when the attachment does not set one.
	 *
	 * <p>Values are stringified rather than left as {@code Object}. Everything
	 * this reads out of a config is a name (of a column, of a field), and a
	 * caller that had to handle "the target column might be a number" would be
	 * handling a case that cannot mean anything.
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> configMap(String key) {
		if (!(config.get(key) instanceof Map<?, ?> nested)) {
			return Map.of();
		}
		Map<String, String> stringified = new LinkedHashMap<>();
		((Map<String, Object>) nested).forEach(
				(nestedKey, value) -> stringified.put(String.valueOf(nestedKey), String.valueOf(value)));
		return stringified;
	}
}
