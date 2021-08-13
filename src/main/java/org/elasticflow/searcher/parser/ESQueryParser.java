package org.elasticflow.searcher.parser;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.search.BooleanClause.Occur;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.QUERY_TYPE;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.field.handler.LongRangeType;
import org.elasticflow.model.EFSearchRequest;
import org.elasticflow.param.end.SearcherParam;
import org.elasticflow.util.Common;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:23
 */
public class ESQueryParser implements QueryParser{

	private final static Logger log = LoggerFactory.getLogger(ESQueryParser.class);

	static public QueryBuilder EmptyQuery() {
		return QueryBuilders.termQuery("EMPTY", "0x000");
	}

	static public BoolQueryBuilder parseRequest(EFSearchRequest request, InstanceConfig instanceConfig) {
		BoolQueryBuilder bquery = QueryBuilders.boolQuery();
		try {
			Map<String, Object> paramMap = request.getParams();
			Set<Entry<String, Object>> entries = paramMap.entrySet();
			Iterator<Entry<String, Object>> iter = entries.iterator();
			/** support fuzzy search */
			int fuzzy = 0;
			if (request.getParam(GlobalParam.PARAM_FUZZY) != null) {
				fuzzy = Integer.parseInt((String) request.getParam(GlobalParam.PARAM_FUZZY));
			}
			while (iter.hasNext()) {
				Entry<String, Object> entry = iter.next();
				String key = entry.getKey();
				Object value = entry.getValue();
				Occur occur = Occur.MUST;
				/** support script search */
				if (key.equalsIgnoreCase(GlobalParam.PARAM_DEFINEDSEARCH)) {
					if (String.valueOf(value).indexOf(GlobalParam.PARAM_ANDSCRIPT) > -1) {
						int pos1 = String.valueOf(value).indexOf(GlobalParam.PARAM_ANDSCRIPT);
						int pos2 = String.valueOf(value).lastIndexOf(GlobalParam.PARAM_ANDSCRIPT);
						if (pos1 != pos2) {
							BoolQueryBuilder bbtmp = QueryBuilders.boolQuery();
							bbtmp.must(getScript(String.valueOf(value).substring(pos1 + GlobalParam.PARAM_ANDSCRIPT.length(), pos2)));
							String qsq = "";
							if (pos1 > 0)
								qsq += String.valueOf(value).substring(0, pos1);
							if (pos2 < String.valueOf(value).length() - GlobalParam.PARAM_ANDSCRIPT.length())
								qsq += String.valueOf(value).substring(pos2 + GlobalParam.PARAM_ANDSCRIPT.length());
							if (qsq.length() > 1)
								bbtmp.must(QueryBuilders.queryStringQuery(qsq));
							bquery.must(bbtmp);
						}
					} else if (String.valueOf(value).indexOf(GlobalParam.PARAM_ORSCRIPT) > -1) {
						int pos1 = String.valueOf(value).indexOf(GlobalParam.PARAM_ORSCRIPT);
						int pos2 = String.valueOf(value).lastIndexOf(GlobalParam.PARAM_ORSCRIPT);
						if (pos1 != pos2) {
							BoolQueryBuilder bbtmp = QueryBuilders.boolQuery();
							bbtmp.should(getScript(String.valueOf(value).substring(pos1 + GlobalParam.PARAM_ORSCRIPT.length(), pos2)));
							String qsq = "";
							if (pos1 > 0)
								qsq += String.valueOf(value).substring(0, pos1);
							if (pos2 < String.valueOf(value).length() - GlobalParam.PARAM_ORSCRIPT.length())
								qsq += String.valueOf(value).substring(pos2 + GlobalParam.PARAM_ORSCRIPT.length());
							if (qsq.length() > 1)
								bbtmp.should(QueryBuilders.queryStringQuery(qsq));
							bquery.must(bbtmp);
						}
					} else {
						QueryStringQueryBuilder _q = QueryBuilders.queryStringQuery(String.valueOf(value));
						bquery.must(_q);
					}
					continue;
				}

				if (key.endsWith(GlobalParam.NOT_SUFFIX)) {
					key = key.substring(0, key.length() - GlobalParam.NOT_SUFFIX.length());
					occur = Occur.MUST_NOT;
				}

				EFField tp = instanceConfig.getWriteField(key);
				SearcherParam sp = instanceConfig.getSearcherParam(key);
				if ((tp == null && sp == null) || Common.isDefaultParam(key)) {
					continue;
				}
				QueryBuilder query = null;
				if (sp != null && sp.getFields() != null && sp.getFields().length() > 0)
					query = buildMultiQuery(sp.getFields(), String.valueOf(value), instanceConfig, request, key, fuzzy);
				else if(tp.getIndextype().contentEquals("dense_vector")) {
					Map<String, Object> _params = new HashMap<>();
					_params.put("query_vector", value);
					Script script = new Script(
					        ScriptType.INLINE,
					        "painless",
					        "(cosineSimilarity(params.query_vector, doc['"+key+"']) + 1.0)", _params); 
					bquery.must(QueryBuilders.scriptScoreQuery(QueryBuilders.matchAllQuery(), script));
				}else {
					query = buildSingleQuery(tp.getAlias(), String.valueOf(value), tp, sp, request, key, fuzzy); 
				}
				if (occur == Occur.MUST_NOT && query != null) {
					bquery.mustNot(query);
					continue;
				}

				if (query != null)
					bquery.must(query);
			}

		} catch (Exception e) {
			log.error("buildBooleanQuery Exception", e);
		}
		return bquery;
	}

	static private void QueryBoost(QueryBuilder query, EFField tp, EFSearchRequest request) throws Exception {
		float boostValue = tp.getBoost();

		Method m = query.getClass().getMethod("boost", new Class[] { float.class });
		if (query instanceof FunctionScoreQueryBuilder)
			boostValue = (float) Math.sqrt(boostValue);
		m.invoke(query, boostValue);
	}

	static private QueryBuilder buildSingleQuery(String key, String value, EFField tp, SearcherParam sp,
			EFSearchRequest request, String paramKey, int fuzzy) throws Exception {
		if (value == null || (tp.getDefaultvalue() == null && value.length() <= 0) || tp == null)
			return null;
		boolean not_analyzed = tp.getAnalyzer().length()>0 ? false : true;

		if (!not_analyzed)
			value = value.toLowerCase().trim();

		BoolQueryBuilder bquery = QueryBuilders.boolQuery();
		String[] values = value.split(",");
		for (String v : values) {
			QueryBuilder query = null;
			if (!not_analyzed || fuzzy>0) {
				query = fieldParserQuery(key, String.valueOf(v), fuzzy);
			} else if (tp.getParamtype().equals("org.elasticflow.field.handler.LongRangeType")) {
				Object _v = Common.parseFieldObject(v, tp);
				LongRangeType val = (LongRangeType) _v; 
				query = QueryBuilders.rangeQuery(key).from(val.getMin()).to(val.getMax())
						.includeLower(sp == null ? true : sp.isIncludeLower())
						.includeUpper(sp == null ? true : sp.isIncludeUpper());
			}  else { 
				query = QueryBuilders.termQuery(key, String.valueOf(v));
				QueryBoost(query, tp, request);
			}

			if (query != null) { 
				if (request.getParams().containsKey(key + "_and"))
					bquery.must(query);
				else
					bquery.should(query);
			}
		}

		return bquery;
	}

	static private QueryBuilder fieldParserQuery(String field, String queryStr, int fuzzy) {
		return fieldParserQuery(field, queryStr, fuzzy, ESSimpleQuery.createQuery(QUERY_TYPE.BOOLEAN_QUERY));
	}

	static private QueryBuilder fieldParserQuery(String field, String queryStr, int fuzzy,
			ESSimpleQuery ESSimpleQuery) {
		if (fuzzy > 0) {
			FuzzyQueryBuilder fzQuery = QueryBuilders.fuzzyQuery(field, queryStr);
			fzQuery.fuzziness(Fuzziness.TWO);
			fzQuery.maxExpansions(fuzzy);
			ESSimpleQuery.add(
					new BoolQueryBuilder().should(fzQuery).should(QueryBuilders.termQuery(field, queryStr).boost(1.2f)),
					"must");
		} else {
			ESSimpleQuery.add(QueryBuilders.termQuery(field, queryStr), "must");
		}
		return ESSimpleQuery.getQuery();
	}

	static private QueryBuilder buildMultiQuery(String multifield, String value, InstanceConfig instanceConfig,
			EFSearchRequest request, String paramKey, int fuzzy) throws Exception {
		DisMaxQueryBuilder bquery = null;
		String[] keys = multifield.split(",");

		if (keys.length <= 0)
			return null;

		if (keys.length == 1) {
			EFField tp = instanceConfig.getWriteField(keys[0]);
			return buildSingleQuery(tp.getAlias(), value, tp, instanceConfig.getSearcherParam(keys[0]), request,
					paramKey, fuzzy);
		}

		String[] word_vals = value.split(",");
		for (String word : word_vals) {
			BoolQueryBuilder subquery2 = null; 
			String val = word;
			DisMaxQueryBuilder parsedDisMaxQuery = null;
			for (String key2 : keys) {
				EFField _tp = instanceConfig.getWriteField(key2);
				QueryBuilder query = buildSingleQuery(_tp.getAlias(),
						_tp.getAnalyzer()!="" ? word : val, _tp,
						instanceConfig.getSearcherParam(key2), request, paramKey, fuzzy);
				if (query != null) {
					if (parsedDisMaxQuery == null)
						parsedDisMaxQuery = QueryBuilders.disMaxQuery()
								.tieBreaker(GlobalParam.DISJUNCTION_QUERY_WEIGHT);
					parsedDisMaxQuery.add(query);
				}
			}
			if (parsedDisMaxQuery != null) {
				if (subquery2 == null)
					subquery2 = QueryBuilders.boolQuery();
				subquery2.must(parsedDisMaxQuery);
			}

			if (subquery2 != null) {
				if (bquery == null)
					bquery = QueryBuilders.disMaxQuery().tieBreaker(GlobalParam.DISJUNCTION_QUERY_WEIGHT);
				bquery.add(subquery2);
			}
		}
		return bquery;
	}

	static private QueryBuilder getScript(String str) {
		return QueryBuilders.scriptQuery( new Script(str.replace("\\", ""))); 
	}
}

class ESSimpleQuery {
	private QueryBuilder innerQuery = null;

	public static ESSimpleQuery createQuery(QUERY_TYPE query_type) {
		QueryBuilder query = null;
		if (query_type == QUERY_TYPE.BOOLEAN_QUERY)
			query = QueryBuilders.boolQuery();
		else if (query_type == QUERY_TYPE.DISJUNCTION_QUERY)
			query = QueryBuilders.disMaxQuery().boost(GlobalParam.DISJUNCTION_QUERY_WEIGHT);
		return new ESSimpleQuery(query);
	}

	private ESSimpleQuery(QueryBuilder query) {
		this.innerQuery = query;
	}

	public void add(QueryBuilder query, String type) {
		if (innerQuery instanceof DisMaxQueryBuilder) {
			((DisMaxQueryBuilder) innerQuery).add(query);
		} else {
			if (type.equals("must"))
				((BoolQueryBuilder) innerQuery).must(query);
			else
				((BoolQueryBuilder) innerQuery).should(query);
		}
	}

	public QueryBuilder getQuery() {
		return innerQuery;
	}
}
