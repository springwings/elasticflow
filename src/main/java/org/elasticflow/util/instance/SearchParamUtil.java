package org.elasticflow.util.instance;

import java.util.*;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.KEY_PARAM;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.model.EFRequest;
import org.elasticflow.model.searcher.SearcherModel;

/**
 * User search parameter processing auxiliary tool
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:14
 */
public class SearchParamUtil {

	public static void normalParam(EFRequest request, SearcherModel<?> SM, InstanceConfig instanceConfig) {
		Object val = request.get(KEY_PARAM.start.name(),
				instanceConfig.getSearcherParam(KEY_PARAM.start.name()), "java.lang.Integer");
		int start = 0;
		int count = 1;
		if (val != null) {
			start = (int) val;
			if (start >= 0)
				SM.setStart(start);
		}
		 
		val = request.get(KEY_PARAM.count.name(),
				instanceConfig.getSearcherParam(KEY_PARAM.count.name()), "java.lang.Integer");
		if (val != null) {
			count = (int) val;
			SM.setCount(count); 
		} 
		
		val = request.get(GlobalParam.CUSTOM_QUERY,
				instanceConfig.getSearcherParam(GlobalParam.CUSTOM_QUERY), "java.lang.String");
		if (val != null) 
			SM.setCustomquery(val.toString());
		

		if ((start + count) > GlobalParam.SEARCH_MAX_WINDOW) 
			request.addError("start+count<=" + GlobalParam.SEARCH_MAX_WINDOW);
		if (request.getParams().containsKey(GlobalParam.PARAM_SHOWQUERY))
			SM.setShowQueryInfo(true);
		if (request.getParams().containsKey(GlobalParam.INSATANCE_STAT))
			SM.setShowStats(true);
	 
		if (request.getParams().containsKey(GlobalParam.HIGHLIGHT_FIELDS)){ 
			SM.setHighlightFields(request.getParam(GlobalParam.HIGHLIGHT_FIELDS));
			if (request.getParams().containsKey(GlobalParam.HIGHLIGHT_TAG))
				SM.setHighlightTag(request.getParam(GlobalParam.HIGHLIGHT_TAG));
		} 
		if (request.getParams().containsKey(GlobalParam.PARAM_FL))
			SM.setFl((String) request.getParam(GlobalParam.PARAM_FL)); 
		if (request.getParams().containsKey(GlobalParam.PARAM_REQUEST_HANDLER))
			SM.setRequestHandler((String) request.getParam(GlobalParam.PARAM_REQUEST_HANDLER));
		if (request.getParams().containsKey(GlobalParam.PARAM_FACET)){
			SM.setFacetParams(getFacetParams(request,instanceConfig));
			HashMap<String, String> FacetExtParams = new HashMap<String, String>(){ 
				private static final long serialVersionUID = -2451479900949006503L;

			{
				put("size","10");
			}};
			SM.setFacetExtParams(FacetExtParams);
			if (request.getParams().containsKey(GlobalParam.PARAM_FACET_EXT)){
				String[] tmp = request.getStringParam(GlobalParam.PARAM_FACET_EXT).split(",");
				FacetExtParams.put("size",tmp[0].split(":")[1]);
				FacetExtParams.put("order",tmp[1].split(":")[1]);
			}
		}
	}

	/**
	 * facet=函数:返回名称:字段名,函数:返回名称:字段名
	 * 
	 * @param rq
	 * @param instanceConfig
	 * @return
	 */
	private static Map<String, String[]> getFacetParams(EFRequest request, InstanceConfig instanceConfig) {
		Map<String, String[]> res = new HashMap<String, String[]>();
		if (request.getParam("facet") != null) {
			for (String pair : ((String) request.getParams().get("facet")).split(",")) { 
				String[] tp = pair.split(":"); 
				res.put(tp[0], tp);
			}
		}
		return res;
	}

}
