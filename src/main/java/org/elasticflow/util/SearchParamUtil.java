package org.elasticflow.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.KEY_PARAM;
import org.elasticflow.field.RiverField;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.RiverRequest;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.param.end.SearcherParam; 

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:14
 */
public class SearchParamUtil {

	public static void normalParam(RiverRequest request, SearcherModel<?, ?, ?> fq,InstanceConfig instanceConfig) {
		Object o = request.get(GlobalParam.KEY_PARAM.start.toString(),
				instanceConfig.getSearcherParam(KEY_PARAM.start.toString()),"java.lang.Integer");
		int start = 0;
		int count = 1;
		if (o != null) {
			start = (int) o;
			if (start >= 0)
				fq.setStart(start);
		}
		o = request.get(KEY_PARAM.count.toString(),
				instanceConfig.getSearcherParam(KEY_PARAM.count.toString()),"java.lang.Integer");
		if (o != null) {
			count = (int) o;
			if (count >= 1 && count <= GlobalParam.SEARCH_MAX_PAGE) {
				fq.setCount(count);
			}
		}
		if((start+count)>GlobalParam.SEARCH_MAX_WINDOW) {
			request.addError("start+count<="+GlobalParam.SEARCH_MAX_WINDOW);
		}
		
		if (request.getParams().containsKey(GlobalParam.PARAM_SHOWQUERY))
			fq.setShowQueryInfo(true);
		if (request.getParams().containsKey(GlobalParam.PARAM_FL))
			fq.setFl(request.getParam(GlobalParam.PARAM_FL));
		if (request.getParams().containsKey(GlobalParam.PARAM_FQ))
			fq.setFq(request.getParam(GlobalParam.PARAM_FQ));
		if (request.getParams().containsKey(GlobalParam.PARAM_REQUEST_HANDLER))
			fq.setRequestHandler(request.getParam(GlobalParam.PARAM_REQUEST_HANDLER));
	}
	
	public static List<SortBuilder<?>> getSortField(RiverRequest request, InstanceConfig instanceConfig) {  
		String sortstrs = request.getParam(KEY_PARAM.sort.toString());
		List<SortBuilder<?>> sortList = new ArrayList<SortBuilder<?>>();
		boolean useScore = false;
		if (sortstrs != null && sortstrs.length() > 0) { 
			boolean reverse = false;
			String[] sortArr = sortstrs.split(",");
			String fieldname = ""; 
			for (String str : sortArr) {
				str = str.trim();
				if (str.endsWith(GlobalParam.SORT_DESC)) {
					reverse = true;
					fieldname = str.substring(0,
							str.indexOf(GlobalParam.SORT_DESC));
				} else if (str.endsWith(GlobalParam.SORT_ASC)) {
					reverse = false;
					fieldname = str.substring(0,
							str.indexOf(GlobalParam.SORT_ASC));
				} else {
					reverse = false;
					fieldname = str;
				}

				switch (fieldname) {
				case GlobalParam.PARAM_FIELD_SCORE:
					sortList.add(SortBuilders.scoreSort().order(
							reverse ? SortOrder.DESC : SortOrder.ASC));
					useScore = true;
					break;
				case GlobalParam.PARAM_FIELD_RANDOM:
					sortList.add(SortBuilders.scriptSort(new Script("random()"), ScriptSortType.NUMBER)); 
					break; 
				default:
					RiverField checked; 
					SearcherParam sp;
					if(instanceConfig.getWriteField(fieldname)!=null && instanceConfig.getWriteField(fieldname).getIndextype().equals("geo_point")) {
						String _tmp = request.getParam(fieldname);
						String[] _geo = _tmp.split(":"); 
						sortList.add(SortBuilders.geoDistanceSort(fieldname,new GeoPoint(Double.parseDouble(_geo[0]), Double.parseDouble(_geo[0]))).order(reverse ? SortOrder.DESC : SortOrder.ASC));
						break;
					}
					if ((checked = instanceConfig.getWriteField(fieldname)) != null) { 
						sortList.add(SortBuilders.fieldSort(checked.getAlias()).order(
								reverse ? SortOrder.DESC : SortOrder.ASC));
					}else if((sp = instanceConfig.getSearcherParam(fieldname))!=null){
						String fields = sp.getFields();
						if(fields!=null) {
							for(String k:fields.split(",")) {
								sortList.add(SortBuilders.fieldSort(k).order(
										reverse ? SortOrder.DESC : SortOrder.ASC));
							}
						}
					}else if(fieldname.equals(GlobalParam.DEFAULT_FIELD)) { 
						sortList.add(SortBuilders.fieldSort(fieldname).order(
								reverse ? SortOrder.DESC : SortOrder.ASC));
					} 
					break;
				}
			}
		} 
		if (!useScore)
			sortList.add(SortBuilders.scoreSort().order(SortOrder.DESC));
		return sortList;
	}
	
	/**
	 * main:funciton:field,son:function:field#new_main:funciton:field
	 * @param rq
	 * @param prs
	 * @return
	 */
		public static Map<String,List<String[]>> getFacetParams(RiverRequest rq,
				InstanceConfig prs) {
			Map<String,List<String[]>> res = new LinkedHashMap<String,List<String[]>>();
			if(rq.getParam("facet")!=null){
				for(String pair:rq.getParams().get("facet").split("#")){
					String[] tmp = pair.split(",");
					List<String[]> son = new ArrayList<>();
					for(String str:tmp) {
						String[] tp = str.split(":");
						if(tp.length>3) {
							String[] tp2= {"","",""};
							for(int i=0;i<tp.length;i++) {
								if(i<2) {
									tp2[i] = tp[i];
								}else {
									if(tp2[2].length()>0) {
										tp2[2] = tp2[2]+":"+tp[i];
									}else {
										tp2[2] = tp[i];
									}
									
								} 
							} 
							son.add(tp2);
						}else {
							son.add(tp);
						}
						
					}
					res.put(tmp[0].split(":")[0], son);
				}
			} 
			return res;
		}

	
}
