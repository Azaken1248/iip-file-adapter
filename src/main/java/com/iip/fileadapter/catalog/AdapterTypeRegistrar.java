package com.iip.fileadapter.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Announces this adapter's <em>type</em> to the Contract Registry's catalog
 * (Phase 6.1).
 *
 * <p>The descriptor is a static resource in this repository, next to the code
 * that reads the config it describes. That adjacency is the whole point: a
 * catalog maintained in the registry would be a second place to edit for every
 * new adapter type and the first thing to go stale, and a config schema that
 * disagrees with the adapter reading it is worse than no schema at all --
 * it produces a form that collects the wrong fields, confidently.
 *
 * <p>Registration is how [UC-9](02-use-cases.md) holds: a new adapter type is a
 * service that starts up and says what it is. Nothing in the registry, the UI,
 * the source service or any other adapter is edited or redeployed.
 *
 * <p><strong>Never fatal, and never retried into a failure.</strong> Being
 * listed in a catalog is a control-plane convenience; writing records is the
 * job. An adapter that refused to start because the registry was briefly down
 * would take the data plane out for a UI dropdown -- so a failure here logs and
 * the next pass tries again.
 *
 * <p>A near-copy of the db-adapter's class of the same name, and knowingly so.
 * Phase 6.2 is the extraction of everything both adapters share, and it is
 * scheduled after this one because [Implementation Plan §1](04-implementation-plan.md)
 * asks for the duplication to exist before the abstraction does. Two copies is
 * the evidence; one shared class written before the second use would be a guess.
 */
public class AdapterTypeRegistrar {

	private static final Logger log = LoggerFactory.getLogger(AdapterTypeRegistrar.class);

	private final RestClient restClient;
	private final String baseUrl;
	private final Map<String, Object> descriptor;
	private final String adapterType;

	private volatile boolean registered;

	public AdapterTypeRegistrar(
			RestClient.Builder restClientBuilder,
			String baseUrl,
			Resource descriptorResource,
			ObjectMapper objectMapper) {

		this.baseUrl = baseUrl;
		this.restClient = restClientBuilder.baseUrl(baseUrl).build();
		this.descriptor = read(descriptorResource, objectMapper);
		this.adapterType = String.valueOf(descriptor.get("type"));
	}

	/**
	 * On the attachment refresh interval, and re-announced rather than
	 * announced once.
	 *
	 * <p>The repeat is not paranoia about lost packets: a registry that is
	 * restarted, restored from a backup, or pointed at a fresh database would
	 * otherwise have an empty catalog until every adapter on the platform
	 * happened to be restarted. Re-announcing means the catalog heals itself,
	 * and the registration is an upsert precisely so it can be repeated
	 * forever.
	 *
	 * <p>Once registered, later passes are quiet -- the log line is worth
	 * seeing on the first success and worth nothing every interval after.
	 */
	@Scheduled(
			initialDelay = 0,
			fixedDelayString = "${iip.catalog.refresh-interval-ms:30000}")
	public void register() {
		try {
			restClient.put()
					.uri("/adapter-types/{type}", adapterType)
					.body(descriptor)
					.retrieve()
					.toBodilessEntity();

			if (!registered) {
				registered = true;
				log.info("Registered adapter type '{}' with the catalog at {}", adapterType, baseUrl);
			}
		}
		catch (RuntimeException e) {
			registered = false;
			log.warn("Could not register adapter type '{}' with {} ({}). Records are unaffected; "
					+ "this type will be missing from the control plane's list of targets until the next attempt.",
					adapterType, baseUrl, e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> read(Resource resource, ObjectMapper objectMapper) {
		try (var stream = resource.getInputStream()) {
			// Unlike the registration itself, this one *is* fatal. The
			// descriptor ships inside the jar, so a failure here means the
			// build is broken rather than the network -- and a service that
			// cannot describe itself has a problem worth failing loudly for.
			return objectMapper.readValue(new String(stream.readAllBytes(), StandardCharsets.UTF_8), Map.class);
		}
		catch (IOException e) {
			throw new UncheckedIOException("adapter-type.json is missing or unreadable on the classpath", e);
		}
	}
}
