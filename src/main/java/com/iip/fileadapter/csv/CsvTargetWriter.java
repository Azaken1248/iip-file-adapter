package com.iip.fileadapter.csv;

import com.iip.fileadapter.attachment.Attachment;
import com.iip.fileadapter.pipeline.RecordEnvelope;
import com.iip.fileadapter.pipeline.TargetWriter;
import org.springframework.stereotype.Component;

/**
 * Writes a record as a line in a file (Phase 6.2, config-driven since 6.3).
 *
 * <p>The one step that makes this service the {@code csv} adapter. Everything
 * around it reads identically to the postgres adapter's.
 *
 * <p>Nothing here knows what an intern is any more, and nothing can: the file
 * to write and the columns to write into it both arrive with the record, from
 * the attachment. That is what turns this from a service that happens to serve
 * one contract into one instance of a catalog type (AD-10).
 */
@Component
public class CsvTargetWriter implements TargetWriter {

	private final FileRecordWriter csvRecordWriter;

	public CsvTargetWriter(FileRecordWriter csvRecordWriter) {
		this.csvRecordWriter = csvRecordWriter;
	}

	@Override
	public void write(RecordEnvelope envelope, Attachment attachment) {
		csvRecordWriter.append(envelope, attachment);
	}
}
