package com.iip.fileadapter.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.kafka.CanonicalEnvelope;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import com.iip.fileadapter.pipeline.TargetWriter;
import org.springframework.stereotype.Component;

/**
 * Writes a record as a line in a file (Phase 6.2).
 *
 * <p>The one step that makes this service the {@code csv} adapter. Everything
 * around it now reads identically to the postgres adapter's, which is what
 * Phase 6.2 was for.
 *
 * <p>It still binds the record to a typed {@link CanonicalEnvelope} to get at
 * intern fields, and so it still knows what an intern is -- the last place in
 * this service that does. That is Phase 6.3's subject: the column list comes
 * from the attachment, and this class stops naming fields at all. Keeping the
 * binding here for now confines the knowledge to the one class the shape says
 * is allowed to be target-specific, rather than leaving it spread through a
 * consumer.
 */
@Component
public class CsvTargetWriter implements TargetWriter {

	private final CsvInternWriter csvInternWriter;
	private final ObjectMapper objectMapper;

	public CsvTargetWriter(CsvInternWriter csvInternWriter, ObjectMapper objectMapper) {
		this.csvInternWriter = csvInternWriter;
		this.objectMapper = objectMapper;
	}

	@Override
	public void write(RecordEnvelope envelope, Attachment attachment) {
		csvInternWriter.append(objectMapper.convertValue(envelope.tree(), CanonicalEnvelope.class));
	}
}
