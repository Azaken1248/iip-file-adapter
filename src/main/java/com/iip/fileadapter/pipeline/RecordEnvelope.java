package com.iip.fileadapter.pipeline;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * The canonical envelope, as every adapter reads it (Phase 6.2, [Data Model
 * §1a](03-data-model.md)).
 *
 * <p>A wrapper over the parsed tree rather than a bound Java type, and that is
 * the whole reason it can be shared. The envelope's fields are identical for
 * every contract on the platform; the payload's are not, and binding the
 * payload would put one contract's field names into a class every contract has
 * to pass through -- which is the coupling Release 5 removed.
 *
 * <p>So the payload stays a {@link JsonNode}. An adapter that needs a field out
 * of it looks the field up by the name its <em>attachment</em> gave, never by a
 * name this class knows.
 *
 * <p>Part of the shape [Architecture §6](01-architecture.md) calls the
 * acceptance checklist for any adapter. The class is deliberately duplicated
 * into each adapter's repository rather than shared as a library: the platform
 * has no artifact repository, and a published jar would make "a new adapter is
 * a new repo" ([UC-9](02-use-cases.md)) mean "a new repo plus a dependency on a
 * version of ours". What keeps the copies honest is Phase 6.10's acceptance
 * suite, which every adapter type must pass.
 */
public record RecordEnvelope(JsonNode tree) {

	public String contractId() {
		return tree.path("contractId").asText();
	}

	public String recordType() {
		return tree.path("recordType").asText();
	}

	public String naturalKey() {
		return tree.path("naturalKey").asText();
	}

	public UUID recordId() {
		return UUID.fromString(tree.path("recordId").asText());
	}

	public Instant occurredAt() {
		return Instant.parse(tree.path("occurredAt").asText());
	}

	public JsonNode payload() {
		return tree.path("payload");
	}

	/**
	 * Whether this record carries the full current state of an entity, to be
	 * applied over whatever is stored, rather than a new record to be added
	 * ([Data Model §2](03-data-model.md)).
	 *
	 * <p>By suffix, because the suffix is the part the platform fixes: §2 ties
	 * each class of record type to a topic name. Anything unrecognized is
	 * create-style, since adding one row too many is visible and fixable where
	 * a wrong upsert silently destroys the row it lands on.
	 */
	public boolean isUpdateStyle() {
		return recordType().endsWith(".updated");
	}
}
