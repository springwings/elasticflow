package org.elasticflow.searcher.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.connection.sockets.ElasticsearchConnector;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.searcher.parser.ESQueryParser;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Main Run Class for Searcher
 * 
 * @author chengwen
 * @version 5.x
 * @date 2018-10-26 09:23
 * @modify 2023-05-26 09:23
 */
public final class ElasticsearchSearcher extends SearcherFlowSocket {
	
	public static boolean crossSubtasks = true;

	public static ElasticsearchSearcher getInstance(ConnectParams connectParams) {
		ElasticsearchSearcher o = new ElasticsearchSearcher();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public void Search(SearcherModel<?> searcherModel, String instance, SearcherHandler handler, EFResponse efResponse)
			throws EFException {
		PREPARE(false, true);
		boolean clearConn = false;
		SearcherResult res = new SearcherResult();
		if (connStatus()) {
			ElasticsearchConnector ESC = null;
			try {
				ESC = (ElasticsearchConnector) GETSOCKET().getConnection(END_TYPE.searcher);
				RestHighLevelClient conn = ESC.getClient();
				List<String> returnFields = new ArrayList<String>();
				if (searcherModel.getFl() != null) {
					for (String s : searcherModel.getFl().split(",")) {
						returnFields.add(s);
					}
				} else {
					Map<String, EFField> tmpFields = instanceConfig.getSearchFields();
					for (Map.Entry<String, EFField> e : tmpFields.entrySet()) {
						if (e.getValue().getStored().equalsIgnoreCase("true"))
							returnFields.add(e.getKey());
					}
				}
				SearchResponse sr = getSearchResponse(conn, searcherModel, returnFields, instance, res);
				if (handler == null) {
					addResult(res, sr, searcherModel, returnFields);
				} else {
					handler.Handle(res, sr, searcherModel, instanceConfig, returnFields);
				}
				if (searcherModel.isShowStats()) {
					MainResponse tmp = conn.info(RequestOptions.DEFAULT);
					JSONObject jo = new JSONObject();
					jo.put("clusterName", tmp.getClusterName());
					jo.put("nodeName", tmp.getNodeName());
					jo.put("version", tmp.getVersion());
					jo.put("tagline", tmp.getTagline());
					res.setStat(jo);
				}
			} catch (Exception e) {
				clearConn = true;
				if (ESC != null) {
					throw Common.convertException(e, this.poolName);
				} else {
					throw Common.convertException(e);
				}
			} finally {
				releaseConn(false, clearConn);
			}
		}
		this.formatResult(res, efResponse);
	}

	private void addResult(SearcherResult res, SearchResponse response, SearcherModel<?> searcherModel,
			List<String> returnFields) {
		SearchHits searchHits = response.getHits();
		res.setTotalHit(searchHits.getTotalHits().value);
		SearchHit[] hits = searchHits.getHits();

		for (SearchHit SH : hits) {
			Map<String, DocumentField> fieldMap = SH.getFields();
			ResponseDataUnit u = ResponseDataUnit.getInstance();
			u.addObject(GlobalParam.RESPONSE_SCORE, SH.getScore());
			Map<String, HighlightField> HfieldMap = SH.getHighlightFields();
			for (Map.Entry<String, DocumentField> e : fieldMap.entrySet()) {
				String name = e.getKey();
				EFField param = instanceConfig.getSearchFields().get(name);
				if (HfieldMap.containsKey(name)) {
					HighlightField v = HfieldMap.get(name);
					StringBuffer _sb = new StringBuffer();
					for (Text text : v.getFragments()) {
						_sb.append(text.string());
					}
					u.addObject(name, _sb.toString());
				} else {
					DocumentField v = e.getValue();
					if (param != null && param.getSeparator() != null) {
						u.addObject(name, v.getValues());
					} else if (returnFields.contains(name)) {
						u.addObject(name, v.getValue());
					}
				}
			}
			if (searcherModel.isShowQueryInfo()) {
				u.addObject(GlobalParam.RESPONSE_EXPLAINS, SH.getExplanation().toString().replace("", ""));
			}
			res.getUnitSet().add(u);
		}

		if (response.getAggregations() != null) {
			res.setFacetInfo(JSON.parseObject(response.toString()).get("aggregations"));
		}
	}

	/**
	 * Build run statements and execute them Facet search for revisions, implemented
	 * through DSL
	 * 
	 * @param conn
	 * @param searcherModel
	 * @param returnFields
	 * @param instance
	 * @param start
	 * @param count
	 * @param sortFields
	 * @param facetBuilders
	 * @param res
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private SearchResponse getSearchResponse(RestHighLevelClient conn, SearcherModel<?> searcherModel,
			List<String> returnFields, String instance, SearcherResult res) throws Exception {
		SearchRequest searchRequest = new SearchRequest(instance);
		ESQueryParser ESP = new ESQueryParser();
		ESP.parseQuery(instanceConfig, searcherModel);
		ESP.parseFilter(instanceConfig, searcherModel);
		ESP.customQueryParse(instanceConfig, searcherModel);
		ESP.getSSB().size(searcherModel.getCount());
		ESP.getSSB().from(searcherModel.getStart());
		List<SortBuilder<?>> sortFields = (List<SortBuilder<?>>) searcherModel.getSortinfo();
		if (sortFields != null)
			for (SortBuilder<?> s : sortFields) {
				ESP.getSSB().sort(s);
			}
		ESP.getSSB().storedFields(returnFields);
		if (searcherModel.isShowQueryInfo()) {
			res.setQueryDetail(JSON.parse(ESP.getSSB().toString()));
			ESP.getSSB().explain(true);
		}
		searchRequest.source(ESP.getSSB());
		SearchResponse response = conn.search(searchRequest, RequestOptions.DEFAULT);
		return response;
	}
}
