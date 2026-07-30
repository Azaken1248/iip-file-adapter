package com.iip.fileadapter.format;

import com.iip.fileadapter.pipeline.RecordEnvelope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * One {@code <record>} element per line (Phase 6.4).
 *
 * <p>A fragment file, not a document, and the distinction is worth stating
 * because it is a real limitation rather than an oversight. A well-formed XML
 * document needs a single root element closed at the end, which an append-only
 * writer cannot provide without rewriting the file's tail on every record --
 * exactly the property that makes appending safe and crash-tolerant. So each
 * line is a complete element and the file as a whole needs wrapping before a
 * strict parser will take it.
 *
 * <p>That is the honest trade for a target that is a growing file. A consumer
 * that needs a document can wrap the fragments; a consumer that streams them --
 * which is most of what reads XML exports -- does not care.
 */
@Component
public class XmlFormatter implements RecordFormatter {

	@Override
	public String id() {
		return "xml";
	}

	@Override
	public Optional<String> header(List<Column> columns) {
		// No declaration line: this is a fragment file, and an XML declaration
		// would promise a document the file is not.
		return Optional.empty();
	}

	@Override
	public String format(RecordEnvelope envelope, List<Column> columns) {
		StringBuilder xml = new StringBuilder("<record>");
		xml.append(element("record_id", envelope.recordId().toString()));
		for (Column column : columns) {
			xml.append(element(column.header(), Values.text(envelope.payload(), column.field())));
		}
		xml.append(element("created_at", envelope.occurredAt().toString()));
		return xml.append("</record>").toString();
	}

	private static String element(String name, String value) {
		String tag = safeTag(name);
		return "<" + tag + ">" + escape(value) + "</" + tag + ">";
	}

	/**
	 * A column header becomes an element name, and an element name has rules a
	 * column header does not. {@code order total} and {@code 1st} are both
	 * reasonable headers and neither is a legal tag, so they are made legal
	 * rather than rejected -- an export that refuses to run because a column was
	 * named with a space would be a poor trade for a name nobody reads.
	 */
	private static String safeTag(String name) {
		String tag = name.replaceAll("[^A-Za-z0-9_.-]", "_");
		return tag.isEmpty() || !Character.isLetter(tag.charAt(0)) && tag.charAt(0) != '_' ? "_" + tag : tag;
	}

	private static String escape(String value) {
		return value.replace("&", "&amp;")
				.replace("<", "&lt;")
				.replace(">", "&gt;")
				.replace("\"", "&quot;");
	}
}
