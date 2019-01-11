package org.elasticflow.model;

import java.util.Map;

import org.elasticflow.field.RiverField;
import org.elasticflow.reader.handler.Handler;

/**
 * JobPage define Task Page Segment
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-08 16:49
 */
public final class Page {

	private String readerScanKey;
	private String readerKey;
	private String start;
	private String end;
	private Handler readHandler;
	private Map<String, RiverField> transField;
	private String additional;

	public static Page getInstance(String readerKey, String readerScanKey, String start, String end,
			Handler readHandler, Map<String, RiverField> transField, String additional) {
		Page o = new Page();
		o.readerKey = readerKey;
		o.readerScanKey = readerScanKey;
		o.start = start;
		o.end = end;
		o.readHandler = readHandler;
		o.transField = transField;
		o.additional = additional;
		return o;
	}

	public String getReaderScanKey() {
		return readerScanKey;
	}

	public void setReaderScanKey(String readerScanKey) {
		this.readerScanKey = readerScanKey;
	}

	public String getReaderKey() {
		return readerKey;
	}

	public void setReaderKey(String readerKey) {
		this.readerKey = readerKey;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	public Handler getReadHandler() {
		return readHandler;
	}

	public void setReadHandler(Handler readHandler) {
		this.readHandler = readHandler;
	}

	public Map<String, RiverField> getTransField() {
		return transField;
	}

	public void setTransField(Map<String, RiverField> transField) {
		this.transField = transField;
	}

	public String getAdditional() {
		return additional;
	}

	public void setAdditional(String additional) {
		this.additional = additional;
	}
}
