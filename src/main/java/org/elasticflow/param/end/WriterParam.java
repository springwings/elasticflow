package org.elasticflow.param.end;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-09 11:36
 */
public class WriterParam {
	private String writeKey;
	/**scan,scan update,unique key update record*/
	private String keyType;

	public String getWriteKey() {
		return writeKey;
	}

	public String getKeyType() {
		return keyType;
	}

	public static void setKeyValue(WriterParam wp, String k, String v) {
		switch (k.toLowerCase()) {
		case "writekey":
			wp.writeKey = v;
			break;
		case "keytype":
			wp.keyType = v;
			break;
		}
	}
}
