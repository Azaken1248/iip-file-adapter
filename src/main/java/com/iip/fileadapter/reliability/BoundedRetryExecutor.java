package com.iip.fileadapter.reliability;

import org.springframework.stereotype.Component;

/**
 * Wraps an action (deserialize + write, in practice) with the classify
 * step from Architecture §6: retriable failures get retried up to
 * maxAttempts with exponential backoff; non-retriable failures -- or a
 * retriable one that's still failing once attempts are exhausted --
 * propagate to the caller, which routes them to the DLQ (see DlqPublisher).
 */
@Component
public class BoundedRetryExecutor {

	private final FailureClassifier classifier;
	private final RetryProperties retryProperties;

	public BoundedRetryExecutor(FailureClassifier classifier, RetryProperties retryProperties) {
		this.classifier = classifier;
		this.retryProperties = retryProperties;
	}

	public void executeWithRetry(Runnable action) {
		int attempt = 0;
		long backoff = retryProperties.getInitialBackoffMs();

		while (true) {
			attempt++;
			try {
				action.run();
				return;
			} catch (RuntimeException e) {
				boolean exhausted = attempt >= retryProperties.getMaxAttempts();
				boolean retriable = classifier.classify(e) == FailureClassification.RETRIABLE;
				if (!retriable || exhausted) {
					throw e;
				}
				sleep(backoff);
				backoff = (long) (backoff * retryProperties.getMultiplier());
			}
		}
	}

	private void sleep(long millis) {
		if (millis <= 0) {
			return;
		}
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("interrupted during retry backoff", e);
		}
	}
}
