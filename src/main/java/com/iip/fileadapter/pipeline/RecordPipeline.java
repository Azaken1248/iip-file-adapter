package com.iip.fileadapter.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.attachment.AttachmentRegistry;
import com.iip.fileadapter.schema.EnvelopeSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * The generic adapter pipeline (Phase 6.2), in the order
 * [Architecture §6](01-architecture.md) draws it:
 *
 * <pre>
 *   deserialize -> contract filter -> validate -> resolve mapping
 *               -> idempotency gate -> transform + write
 * </pre>
 *
 * <p>with classify / retry / DLQ wrapped around the whole thing by the caller.
 * §6 calls this diagram "the acceptance checklist for any new adapter", and
 * this class exists so that checklist is a thing you can read rather than a
 * thing you have to reconstruct from a consumer method.
 *
 * <p><strong>Why this is a duplicated shape and not a shared library.</strong>
 * Extracting it into a jar would give one place to fix a bug, and would cost
 * more than it gives here: this platform has no artifact repository, every
 * adapter builds its own image from its own repo, and
 * [UC-9](02-use-cases.md)'s promise is that a new adapter type is a new
 * service -- not a new service plus a published dependency on the version of
 * the shape the existing adapters happen to be on. The copies are kept honest
 * instead by Phase 6.10's acceptance suite: every adapter type must pass the
 * same idempotency, DLQ and contract-filter tests, so a copy that drifts fails
 * a build rather than a promise.
 *
 * <p>What is <em>not</em> here is as deliberate as what is. There is no
 * transform step of its own, because for both existing adapters the transform
 * is inseparable from the write (a column mapping is applied by the INSERT that
 * uses it); and there is no retry loop, because retrying is a property of the
 * <em>delivery</em> of a record rather than of a record's handling, and belongs
 * where the offset is.
 */
@Component
public class RecordPipeline {

	private static final Logger log = LoggerFactory.getLogger(RecordPipeline.class);

	private final ObjectMapper objectMapper;
	private final AttachmentRegistry attachments;
	private final EnvelopeSchema envelopeSchema;
	private final IdempotencyGate idempotencyGate;
	private final TargetWriter targetWriter;

	public RecordPipeline(
			ObjectMapper objectMapper,
			AttachmentRegistry attachments,
			EnvelopeSchema envelopeSchema,
			IdempotencyGate idempotencyGate,
			TargetWriter targetWriter) {

		this.objectMapper = objectMapper;
		this.attachments = attachments;
		this.envelopeSchema = envelopeSchema;
		this.idempotencyGate = idempotencyGate;
		this.targetWriter = targetWriter;
	}

	/**
	 * Applies one record, or decides it is not this adapter's to apply.
	 *
	 * <p>Throws on anything that should be retried or quarantined. Returning
	 * normally means the offset may commit -- which is true both of a record
	 * that was written and of one that was skipped, and the pipeline is
	 * deliberately unable to tell the caller which, because the caller must not
	 * treat them differently.
	 */
	public void apply(String json) {
		// 1. Deserialize. A tree, not a type: the payload's shape belongs to a
		// contract, and the pipeline serves all of them.
		RecordEnvelope envelope = new RecordEnvelope(parse(json));

		// 2. Contract filter. Before validation, because on a pattern
		// subscription most of what any adapter sees belongs to someone else,
		// and quarantining another adapter's malformed record would put one
		// copy of every poison message in the DLQ per adapter on the platform.
		Optional<Attachment> attachment = attachments.find(envelope.contractId());
		if (attachment.isEmpty()) {
			// DEBUG, not INFO: on a platform with several contracts this is the
			// majority of what every adapter sees, and a line per skipped
			// record would bury the ones that matter.
			log.debug("Skipping a '{}' record: this adapter is not attached to that contract",
					envelope.contractId());
			return;
		}

		// 3. Validate, now that the record is known to be ours. Everything
		// below is entitled to assume the envelope's shape, which is what
		// "rejected at the boundary, not deep in business logic" has to mean.
		envelopeSchema.validate(json);

		// 4/5. Resolve the mapping and consult the gate. Both take the
		// attachment, because one instance of this adapter serves every
		// contract attached to it.
		if (!idempotencyGate.shouldApply(envelope, attachment.get())) {
			log.debug("Skipping record {}: already applied", envelope.recordId());
			return;
		}

		// 6. The only step that knows what kind of target this is.
		targetWriter.write(envelope, attachment.get());

		idempotencyGate.markApplied(envelope, attachment.get());
	}

	private com.fasterxml.jackson.databind.JsonNode parse(String json) {
		try {
			return objectMapper.readTree(json);
		} catch (JsonProcessingException e) {
			// Unparseable bytes cannot be attributed to a contract at all, so
			// this is the one failure every adapter on the platform handles
			// identically -- none of them can tell whose record it was.
			throw new RuntimeException(e);
		}
	}
}
