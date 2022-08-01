package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.List;
 
/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-07-22 09:08
 */
public class SearcherResult {
	
	private float useTime;
	private String callDateTime; 
	private long totalHit;
	private List<ResponseDataUnit> unitSet;
	private Object facetInfo=null;   
	private Object queryDetail = null;
	private Object explainInfo;
	private Object stat;
	private boolean success = true;
	private String errorInfo = "";
	
	public SearcherResult() {
		unitSet = new ArrayList<ResponseDataUnit>();
	}

	public List<ResponseDataUnit> getUnitSet() {
		return unitSet;
	}

	public void setUnitSet(List<ResponseDataUnit> unitSet) {
		this.unitSet = unitSet;
	}

	public float getUseTime() {
		return useTime;
	}

	public void setUseTime(float useTime) {
		this.useTime = useTime;
	}

	public String getCallDateTime() {
		return callDateTime;
	}

	public void setCallDateTime(String callDateTime) {
		this.callDateTime = callDateTime;
	}

	public long getTotalHit() {
		return totalHit;
	}

	public void setTotalHit(long totalHit) {
		this.totalHit = totalHit;
	}

	public Object getFacetInfo() {
		return facetInfo;
	}

	public void setFacetInfo(Object facetInfo) {
		this.facetInfo = facetInfo;
	}  
 

	public Object getQueryDetail() {
		return queryDetail;
	}

	public void setQueryDetail(Object queryDetail) {
		this.queryDetail = queryDetail;
	}

	public Object getExplainInfo() {
		return explainInfo;
	}

	public void setExplainInfo(Object explainInfo) {
		this.explainInfo = explainInfo;
	}	

	public Object getStat() {
		return stat;
	}

	public void setStat(Object stat) {
		this.stat = stat;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getErrorInfo() {
		return errorInfo;
	}

	public void setErrorInfo(String errorInfo) {
		this.errorInfo = errorInfo;
	}   
}