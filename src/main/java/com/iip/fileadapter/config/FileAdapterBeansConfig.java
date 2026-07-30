package com.iip.fileadapter.config;

import com.iip.fileadapter.csv.CsvInternReader;
import com.iip.fileadapter.csv.FileRecordWriter;
import com.iip.fileadapter.format.RecordFormatters;
import com.iip.fileadapter.dedup.DedupStores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
public class FileAdapterBeansConfig {

	/**
	 * Phase 6.3: the path here is only a *default*, used by an attachment that
	 * names no file of its own. The deployment still has one so that an
	 * existing single-contract install keeps writing where it always did.
	 */
	@Bean
	public FileRecordWriter fileRecordWriter(
			@Value("${iip.file.output-path}") String outputPath,
			RecordFormatters formatters) {
		return new FileRecordWriter(Path.of(outputPath), formatters);
	}

	@Bean
	public CsvInternReader csvInternReader(@Value("${iip.file.output-path}") String outputPath) {
		return new CsvInternReader(Path.of(outputPath));
	}

	/**
	 * Phase 6.3: one store per contract, under this directory. Serving several
	 * contracts from one instance makes the split necessary -- replaying one
	 * contract must not mean forgetting what every other contract was sent.
	 */
	@Bean
	public DedupStores dedupStores(@Value("${iip.file.dedup-store-path}") String dedupStorePath) {
		return new DedupStores(Path.of(dedupStorePath));
	}
}
