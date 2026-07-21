package com.iip.fileadapter;

import org.springframework.boot.SpringApplication;

public class TestFileAdapterApplication {

	public static void main(String[] args) {
		SpringApplication.from(FileAdapterApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
