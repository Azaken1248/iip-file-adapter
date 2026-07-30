package com.iip.fileadapter.format;

import com.iip.fileadapter.attachment.AttachmentConfigurationException;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The formats this adapter offers, by the id an attachment selects them with
 * (Phase 6.4).
 *
 * <p>Built from whatever {@link RecordFormatter} beans exist rather than a
 * hardcoded list, so adding a format is adding a class. That is the same
 * relationship the platform has with adapter types one level up: the catalog
 * is assembled from what is deployed, not maintained alongside it.
 */
@Component
public class RecordFormatters {

	private final Map<String, RecordFormatter> byId;

	public RecordFormatters(List<RecordFormatter> formatters) {
		this.byId = formatters.stream().collect(Collectors.toMap(
				RecordFormatter::id, Function.identity(), (a, b) -> a, LinkedHashMap::new));
	}

	/**
	 * @throws AttachmentConfigurationException naming what is available, since
	 *     the mistake is almost always a typo or a format this deployment does
	 *     not have -- and a message that lists the options answers both.
	 */
	public RecordFormatter require(String id) {
		RecordFormatter formatter = byId.get(id);
		if (formatter == null) {
			throw new AttachmentConfigurationException(
					"unknown format '" + id + "'; this adapter can write " + byId.keySet());
		}
		return formatter;
	}

	public List<String> available() {
		return List.copyOf(byId.keySet());
	}
}
