package com.iip.fileadapter.reliability;

import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedRetryExecutorTest {

	// Zero backoff keeps the test fast; retry *count* and give-up behavior
	// are what's under test here, not real timing.
	private final RetryProperties fastRetryProperties = new RetryProperties(4, 0, 1.0);
	private final BoundedRetryExecutor executor = new BoundedRetryExecutor(new FailureClassifier(), fastRetryProperties);

	@Test
	void retriesARetriableFailureUpToMaxAttemptsThenGivesUp() {
		AtomicInteger invocations = new AtomicInteger(0);

		assertThrows(RuntimeException.class, () -> executor.executeWithRetry(() -> {
			invocations.incrementAndGet();
			throw new RuntimeException("transient", new ConnectException("Connection refused"));
		}));

		assertEquals(4, invocations.get(), "expected exactly maxAttempts invocations before giving up");
	}

	@Test
	void nonRetriableFailureIsNotRetried() {
		AtomicInteger invocations = new AtomicInteger(0);

		assertThrows(RuntimeException.class, () -> executor.executeWithRetry(() -> {
			invocations.incrementAndGet();
			throw new RuntimeException("permanent", new IllegalStateException("malformed"));
		}));

		assertEquals(1, invocations.get(), "a non-retriable failure should fail on the first attempt, no retries");
	}

	@Test
	void succeedsOnceTheUnderlyingFailureStopsHappening() {
		AtomicInteger invocations = new AtomicInteger(0);

		executor.executeWithRetry(() -> {
			int attempt = invocations.incrementAndGet();
			if (attempt < 3) {
				throw new RuntimeException("transient", new ConnectException("Connection refused"));
			}
		});

		assertEquals(3, invocations.get(), "expected it to stop retrying as soon as the action succeeds");
	}

	@Test
	void aPositiveBackoffActuallyDelaysBetweenAttempts() {
		RetryProperties withBackoff = new RetryProperties(3, 50, 1.0);
		BoundedRetryExecutor slowExecutor = new BoundedRetryExecutor(new FailureClassifier(), withBackoff);
		AtomicInteger invocations = new AtomicInteger(0);

		long start = System.currentTimeMillis();
		assertThrows(RuntimeException.class, () -> slowExecutor.executeWithRetry(() -> {
			invocations.incrementAndGet();
			throw new RuntimeException("transient", new ConnectException("Connection refused"));
		}));
		long elapsed = System.currentTimeMillis() - start;

		// 2 backoff windows between 3 attempts, 50ms each -- allow slack for
		// scheduling jitter rather than asserting an exact figure.
		assertTrue(elapsed >= 90, "expected at least ~100ms of real backoff delay, took " + elapsed + "ms");
	}
}
