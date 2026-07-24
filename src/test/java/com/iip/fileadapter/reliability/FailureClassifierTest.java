package com.iip.fileadapter.reliability;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FailureClassifierTest {

	private final FailureClassifier classifier = new FailureClassifier();

	static Stream<Arguments> exceptionTable() {
		return Stream.of(
				Arguments.of("connection refused", new ConnectException("Connection refused"), FailureClassification.RETRIABLE),
				Arguments.of("generic I/O failure (disk hiccup, permission race, etc. -- the file-adapter analogue of a DB connection blip)",
						new IOException("Too many open files"), FailureClassification.RETRIABLE),
				Arguments.of("deserialization error", new JsonParseException(null, "Unexpected character"), FailureClassification.NON_RETRIABLE),
				Arguments.of("output/dedup path doesn't exist (misconfiguration, not a transient hiccup -- and itself an IOException subtype, "
						+ "so it must be checked before the generic IOException rule or that rule would shadow it)",
						new NoSuchFileException("/data/interns.csv"), FailureClassification.NON_RETRIABLE),
				Arguments.of("an unrecognized exception type defaults to non-retriable",
						new IllegalStateException("something we've never seen"), FailureClassification.NON_RETRIABLE)
		);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("exceptionTable")
	void classifiesKnownExceptionTypes(String description, Throwable error, FailureClassification expected) {
		assertEquals(expected, classifier.classify(error));
	}

	@Test
	void walksTheCauseChainToFindAClassifiableException() {
		Exception wrapped = new RuntimeException("wrapper", new ConnectException("Connection refused"));
		assertEquals(FailureClassification.RETRIABLE, classifier.classify(wrapped));
	}
}
