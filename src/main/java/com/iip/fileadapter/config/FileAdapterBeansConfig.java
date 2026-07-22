package com.iip.fileadapter.config;

import com.iip.fileadapter.csv.CsvInternReader;
import com.iip.fileadapter.csv.CsvInternWriter;
import com.iip.fileadapter.dedup.DedupStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

@Configuration
public class FileAdapterBeansConfig {

	@Bean
	public CsvInternWriter csvInternWriter(@Value("${iip.file.output-path}") String outputPath) {
		return new CsvInternWriter(Path.of(outputPath));
	}

	@Bean
	public CsvInternReader csvInternReader(@Value("${iip.file.output-path}") String outputPath) {
		return new CsvInternReader(Path.of(outputPath));
	}

	@Bean
	public DedupStore dedupStore(@Value("${iip.file.dedup-store-path}") String dedupStorePath) {
		return new DedupStore(Path.of(dedupStorePath));
	}
}
