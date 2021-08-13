package org.elasticflow.model;

import org.elasticflow.config.InstanceConfig;

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
	private InstanceConfig instanceConfig;
	private String additional;

	public static Page getInstance(String readerKey, String readerScanKey, String start, String end,
			 InstanceConfig instanceConfig, String additional) {
		Page o = new Page();
		o.readerKey = readerKey;
		o.readerScanKey = readerScanKey;
		o.start = start;
		o.end = end;
		o.instanceConfig = instanceConfig;
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

	public String getEnd() {
		return end;
	}

	public InstanceConfig getInstanceConfig() {
		return this.instanceConfig;
	} 

	public String getAdditional() {
		return additional;
	} 
}
