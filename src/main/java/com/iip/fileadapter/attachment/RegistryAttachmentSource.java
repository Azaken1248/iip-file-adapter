package com.iip.fileadapter.attachment;

import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Fetches this adapter's attachments from the Contract Registry over HTTP
 * (Phase 5.2).
 *
 * <p>Over HTTP, and not by querying {@code adapter_attachments} directly,
 * even though this service already holds a connection to the same physical
 * Postgres. [Architecture §4](01-architecture.md) draws the db-adapter reading
 * its target mapping <em>from the Contract Registry</em>, and the deployment
 * view models that table as the registry's own. A direct read would work today
 * and would make this service depend on another service's column names.
 */
public class RegistryAttachmentSource implements AttachmentSource {

	private static final String ADAPTER_TYPE = "csv";

	private final RestClient restClient;
	private final String baseUrl;

	public RegistryAttachmentSource(RestClient.Builder restClientBuilder, String baseUrl) {
		this.baseUrl = baseUrl;
		this.restClient = restClientBuilder.baseUrl(baseUrl).build();
	}

	@Override
	public String describe() {
		return "Contract Registry at " + baseUrl + " (adapter type '" + ADAPTER_TYPE + "')";
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<Attachment> loadAll() {
		List<Map<String, Object>> documents;
		try {
			documents = restClient.get()
					.uri("/adapters?type={type}", ADAPTER_TYPE)
					.retrieve()
					.body(List.class);
		}
		catch (RestClientException e) {
			throw new AttachmentRegistryUnavailableException(baseUrl, e);
		}

		if (documents == null) {
			return List.of();
		}

		List<Attachment> attachments = new ArrayList<>();
		for (Map<String, Object> document : documents) {
			attachments.add(new Attachment(
					String.valueOf(document.get("attachmentId")),
					String.valueOf(document.get("contractId")),
					document.get("config") instanceof Map<?, ?> config
							? (Map<String, Object>) config
							: Map.of()));
		}
		return attachments;
	}
}
