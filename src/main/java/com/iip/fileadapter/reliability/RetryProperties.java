package com.iip.fileadapter.reliability;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "iip.retry")
public class RetryProperties {

	private int maxAttempts = 3;
	private long initialBackoffMs = 200;
	private double multiplier = 2.0;

	public RetryProperties() {
	}

	public RetryProperties(int maxAttempts, long initialBackoffMs, double multiplier) {
		this.maxAttempts = maxAttempts;
		this.initialBackoffMs = initialBackoffMs;
		this.multiplier = multiplier;
	}

	public int getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(int maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	public long getInitialBackoffMs() {
		return initialBackoffMs;
	}

	public void setInitialBackoffMs(long initialBackoffMs) {
		this.initialBackoffMs = initialBackoffMs;
	}

	public double getMultiplier() {
		return multiplier;
	}

	public void setMultiplier(double multiplier) {
		this.multiplier = multiplier;
	}
}
