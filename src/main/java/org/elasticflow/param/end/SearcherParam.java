package org.elasticflow.param.end;


/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-01-22 09:08
 */
public class SearcherParam {
	
	private String name = null;
	private boolean includeLower = true;
	private boolean includeUpper = true;
	private String analyzer = "";
	private float boost = 1.0f;
	private String defaultValue = null;
	private String fields = null;
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getAnalyzer() {
		return analyzer;
	}
	
	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}
	
	public float getBoost() {
		return boost;
	}

	public void setBoost(String boost) {
		this.boost = Float.valueOf(boost);
	} 
	
	public String getDefaultValue() {
		return this.defaultValue;
	} 

	public void setDefaultValue(String value) {
		this.defaultValue = value;
	} 
	
	public String getFields() {
		return fields;
	}

	public void setFields(String fields) {
		this.fields = fields;
	}

	public boolean isIncludeLower() {
		return includeLower;
	}

	public void setIncludeLower(String includeLower) {
		if (includeLower.toLowerCase().equals("false"))
			this.includeUpper = false;
	}

	public boolean isIncludeUpper() {
		return includeUpper;
	}

	public void setIncludeUpper(String includeUpper) {
		if (includeUpper.toLowerCase().equals("false"))
			this.includeUpper = false;
	}
}
