package org.elasticflow.param.end;

import org.elasticflow.param.warehouse.ScanParam;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2019-01-11 15:13
 * @modify 2019-01-11 15:13
 */
public class MessageParam extends ScanParam {
	private String se;
	private String type;
	private String topic;
	private String handler;

	public String getSe() {
		return se;
	}

	public void setSe(String se) {
		this.se = se;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getHandler() {
		return handler;
	}

	public void setHandler(String handler) {
		this.handler = handler;
	}

}
