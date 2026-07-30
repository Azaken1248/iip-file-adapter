package com.iip.fileadapter.format;

/**
 * One column of a formatted record: the name to write it under, and the payload
 * field to read it from (Phase 6.3, moved here by 6.4).
 *
 * <p>"Column" is CSV vocabulary and the name is kept deliberately even though
 * the JSON and XML formatters use the same list -- what it describes is the
 * <em>selection and order of fields</em> an attachment wants written, which is
 * the same decision whatever the output looks like. A second vocabulary per
 * format would mean a second config shape per format.
 */
public record Column(String header, String field) {
}
