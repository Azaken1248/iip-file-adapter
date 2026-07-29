package com.iip.fileadapter.reliability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.iip.fileadapter.schema.EnvelopeSchemaViolationException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.NoSuchFileException;
import java.util.List;

/**
 * Distinguishes transient failures (worth retrying -- see Architecture §6)
 * from permanent ones (retrying can never succeed). Mirrors db-adapter's
 * classifier in shape; the specific rules differ since this adapter's
 * write target is local disk, not a network database -- a generic
 * IOException is the realistic transient-failure analogue of db-adapter's
 * TransientDataAccessException here.
 *
 * The classification is data, not a chain of if/else: RULES is evaluated
 * in declared order and the first match wins, at every level of the cause
 * chain (frameworks routinely wrap the actually-informative exception).
 * Order matters because the candidate types aren't disjoint --
 * JsonProcessingException and NoSuchFileException are themselves
 * IOException subtypes, so both must be listed before the broad
 * IOException rule below or it would silently shadow them (this is
 * exactly the bug Phase 1.17 found for real: the first draft here checked
 * the retriable IOException case before the non-retriable one).
 */
@Component
public class FailureClassifier {

	private static final List<ClassificationRule> RULES = List.of(
			new ClassificationRule(EnvelopeSchemaViolationException.class, FailureClassification.NON_RETRIABLE,
					"the message doesn't match the registered envelope schema -- the bytes and the schema are both "
							+ "fixed for the lifetime of this attempt, so every retry would fail identically. Listed "
							+ "explicitly rather than left to the unknown-type default below, because the default is "
							+ "a safety net and this is a decision"),
			new ClassificationRule(JsonProcessingException.class, FailureClassification.NON_RETRIABLE,
					"malformed/unparseable message payload -- reprocessing identical bytes can't fix this"),
			new ClassificationRule(NoSuchFileException.class, FailureClassification.NON_RETRIABLE,
					"output path or dedup store path doesn't exist -- a misconfiguration, not a transient disk hiccup; "
							+ "retrying the identical write can't fix a path that was never there"),

			new ClassificationRule(ConnectException.class, FailureClassification.RETRIABLE,
					"target unreachable at the TCP level (relevant if the output path is itself network-mounted)"),
			new ClassificationRule(IOException.class, FailureClassification.RETRIABLE,
					"generic I/O failure (disk hiccup, transient permission race, etc.) -- the file-adapter analogue "
							+ "of a DB connection blip"));

	public FailureClassification classify(Throwable error) {
		for (Throwable current = error; current != null; current = current.getCause()) {
			for (ClassificationRule rule : RULES) {
				if (rule.type().isInstance(current)) {
					return rule.classification();
				}
			}
		}
		// Unknown failure types default to non-retriable: retrying
		// something we don't understand risks masking a real bug behind
		// repeated no-op attempts, which is worse than quarantining it.
		return FailureClassification.NON_RETRIABLE;
	}

	private record ClassificationRule(Class<? extends Throwable> type, FailureClassification classification, String rationale) {
	}
}
