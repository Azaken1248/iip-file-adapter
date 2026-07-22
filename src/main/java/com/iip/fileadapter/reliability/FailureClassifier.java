package com.iip.fileadapter.reliability;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ConnectException;

/**
 * Distinguishes transient failures (worth retrying -- see Architecture §6)
 * from permanent ones (retrying can never succeed). Mirrors db-adapter's
 * classifier in shape; the specific retriable/non-retriable types differ
 * since this adapter's write target is local disk, not a network database
 * -- a generic IOException is the realistic transient-failure analogue of
 * db-adapter's TransientDataAccessException here. Walks the cause chain
 * since frameworks routinely wrap the actually-informative exception.
 */
@Component
public class FailureClassifier {

	public FailureClassification classify(Throwable error) {
		for (Throwable current = error; current != null; current = current.getCause()) {
			// Non-retriable checked first: JsonProcessingException is
			// itself an IOException subtype, so the broader retriable
			// check would otherwise shadow it before it's ever reached.
			if (isNonRetriable(current)) {
				return FailureClassification.NON_RETRIABLE;
			}
			if (isRetriable(current)) {
				return FailureClassification.RETRIABLE;
			}
		}
		// Unknown failure types default to non-retriable: retrying
		// something we don't understand risks masking a real bug behind
		// repeated no-op attempts, which is worse than quarantining it.
		return FailureClassification.NON_RETRIABLE;
	}

	private boolean isRetriable(Throwable error) {
		return error instanceof ConnectException
				|| error instanceof IOException;
	}

	private boolean isNonRetriable(Throwable error) {
		return error instanceof JsonProcessingException;
	}
}
