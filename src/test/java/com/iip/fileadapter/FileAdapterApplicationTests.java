package com.iip.fileadapter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class FileAdapterApplicationTests {

	// Without this, CsvInternWriter/DedupStore beans fall back to
	// application.yml's default ./data/... paths and write into the repo's
	// real working directory on every test run.
	@TempDir
	static Path tempDir;

	@DynamicPropertySource
	static void fileProperties(DynamicPropertyRegistry registry) {
		registry.add("iip.file.output-path", () -> tempDir.resolve("interns.csv").toString());
		registry.add("iip.file.dedup-store-path", () -> tempDir.resolve("dedup").toString());
	}

	@Test
	void contextLoads() {
	}

}
