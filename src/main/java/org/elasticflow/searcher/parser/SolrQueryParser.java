package org.elasticflow.searcher.parser;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.RiverRequest;
import org.elasticflow.param.end.SearcherParam;
import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:24
 */
public class SolrQueryParser implements QueryParser{

	public static SolrQuery parseRequest(RiverRequest request, InstanceConfig prs) {
		SolrQuery sq = new SolrQuery();
		StringBuilder qu = new StringBuilder();
		Map<String, String> paramMap = request.getParams();
		Set<Entry<String, String>> entries = paramMap.entrySet();
		Iterator<Entry<String, String>> iter = entries.iterator();
		boolean start = true;
		while (iter.hasNext()) {
			Entry<String, String> entry = iter.next();
			String k = entry.getKey();
			String v = entry.getValue();

			if (Common.isDefaultParam(k)) {
				switch (k) {
				case "sort":
					sq.setParam("sort", getSortInfo(v, sq));
					break;
				case "group":
					sq.setParam("group", getGroupInfo(v, sq));
					sq.setParam("group.ngroups", "true");
					break; 
				case "facet":
					sq.setParam("facet", getFacetInfo(v, sq));
					break;
				}
				continue;
			}
			
			if(k.equals(GlobalParam.PARAM_DEFINEDSEARCH)){ 
				if (!start) {
					qu.append(" AND ");
				} 
				qu.append("("+v+")");
				start=false;
				continue;
			}
			
			RiverField tp = prs.getWriteField(k);
			SearcherParam sp = prs.getSearcherParam(k);
			if (tp == null && sp==null){
				continue; 
			}  
			if (!start) {
				qu.append(" AND ");
			}   
			if(sp!=null && sp.getFields() != null && sp.getFields().length() > 0){
				qu.append(buildMultiQuery(v, sp.getFields(), paramMap));
			}else{
				qu.append(buildSingleQuery(tp.getAlias(), v));
			} 
			start = false;
		}
		if (qu.length() < 1) {
			sq.setQuery("*:*");
		} else {
			sq.setQuery("_query_:" + qu);
		}
		return sq;
	} 
	
	static String buildMultiQuery(String v,String combineSearch,Map<String, String> paramMap){ 
		StringBuilder sb = new StringBuilder();
		for(String k:combineSearch.split(",")){
			sb.append(buildSingleQuery(k,v));
		}
		return sb.toString();
	}
	
	static String buildSingleQuery(String k,String v){
		if (k.endsWith(GlobalParam.NOT_SUFFIX)) {
			k = k.substring(0, k.length() - GlobalParam.NOT_SUFFIX.length());
			return "( NOT " + k + ":" + v + ")";
		} else { 
			return "(" + k + ":" + v + ")";
		}
	}
	static String getGroupInfo(String strs, SolrQuery sq) {
		for (String s : strs.split(",")) {
			String[] tmp = s.split(":");
			if (tmp.length != 2) {
				continue;
			}
			if (tmp[0].equals("group.field")) {
				sq.setParam("group.field", tmp[1]);
			}
			if (tmp[0].equals("group.query")) {
				sq.setParam("group.query", tmp[1]);
			}
			if (tmp[0].equals("group.ngroups")) {
				sq.setParam("group.ngroups", tmp[1]);
			}
			if (tmp[0].equals("group.limit")) {
				sq.setParam("group.limit", tmp[1]);
			}
			if (tmp[0].equals("group.sort")) {
				sq.setParam("group.sort", tmp[1]);
			}
		}
		return "true";
	}

	static String getFacetInfo(String strs, SolrQuery sq) {
		for (String s : strs.split(";")) {
			String[] tmp = s.split(":");
			if (tmp.length <2) {
				continue;
			}
			String key = tmp[0].toLowerCase();
			tmp = Arrays.copyOfRange(tmp, 1, tmp.length);
			switch (key) {
			case "facet.query": 
				sq.setParam(key, StringUtils.join(tmp, ":").split(","));
				break; 
			default:
				sq.setParam(key, StringUtils.join(tmp, ":"));
				break;
			} 
		}
		return "true";
	}

	static String getSortInfo(String strs, SolrQuery sq) {
		StringBuilder sf = new StringBuilder();
		sf.append("score desc");
		for (String str : strs.split(",")) {
			str = str.trim();
			if (str.endsWith(GlobalParam.SORT_DESC)) {
				sf.append(",");
				sf.append(str.substring(0, str.indexOf(GlobalParam.SORT_DESC)));
				sf.append(" desc"); 
			} else if (str.endsWith(GlobalParam.SORT_ASC)) {
				sf.append(",");
				sf.append(str.substring(0, str.indexOf(GlobalParam.SORT_ASC)));
				sf.append(" asc"); 
			}
		}
		return sf.toString();
	}

}
