package com.iip.fileadapter.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/**
 * Mirrors source-service's and db-adapter's WebConfig: Spring Boot doesn't
 * enable CORS by default, and the UI (a different origin/port) needs it to
 * call this adapter's admin API from the browser at all.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

	private final List<String> allowedOrigins;

	public WebConfig(@Value("${iip.cors.allowed-origins}") List<String> allowedOrigins) {
		this.allowedOrigins = allowedOrigins;
	}

	@Override
	public void addCorsMappings(CorsRegistry registry) {
		registry.addMapping("/interns/**")
				.allowedOrigins(allowedOrigins.toArray(new String[0]))
				.allowedMethods("GET")
				.allowedHeaders("*");
		registry.addMapping("/admin/**")
				.allowedOrigins(allowedOrigins.toArray(new String[0]))
				.allowedMethods("GET", "POST")
				.allowedHeaders("*");
	}
}
