package org.elasticflow.model.searcher;

import java.util.ArrayList;
import java.util.List;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFRequest;
import org.elasticflow.param.end.SearcherParam;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @description
 * @author chengwen
 * @version 5.0
 * @date 2023-02-22 09:08
 */
public class SearcherElasticsearchModel extends SearcherModel<SortBuilder<?>> {
	 
	private List<SortBuilder<?>> sortinfo;  
	 
	public static SearcherElasticsearchModel getInstance(EFRequest request, InstanceConfig instanceConfig) {
		SearcherElasticsearchModel SM = new SearcherElasticsearchModel(); 
		SM.setRequestHandler("");
		SM.setSorts(SearcherElasticsearchModel.getSortField(request, instanceConfig));
		SM.setEfRequest(request);
		return SM;
	}

	public static List<SortBuilder<?>> getSortField(EFRequest request, InstanceConfig instanceConfig) {
		String sortstrs = (String) request.getParam(GlobalParam.KEY_PARAM.sort.name());
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
					fieldname = str.substring(0, str.indexOf(GlobalParam.SORT_DESC));
				} else if (str.endsWith(GlobalParam.SORT_ASC)) {
					reverse = false;
					fieldname = str.substring(0, str.indexOf(GlobalParam.SORT_ASC));
				} else {
					reverse = false;
					fieldname = str;
				}

				switch (fieldname) {
					case GlobalParam.PARAM_FIELD_SCORE:
						sortList.add(SortBuilders.scoreSort().order(reverse ? SortOrder.DESC : SortOrder.ASC));
						useScore = true;
						break;
					case GlobalParam.PARAM_FIELD_RANDOM:
						sortList.add(SortBuilders.scriptSort(new Script("random()"), ScriptSortBuilder.ScriptSortType.NUMBER));
						break;
					default:
						EFField checked;
						SearcherParam sp;
						if (instanceConfig.getWriteField(fieldname) != null
								&& instanceConfig.getWriteField(fieldname).getIndextype().equals("geo_point")) {
							String _tmp = (String) request.getParam(fieldname);
							String[] _geo = _tmp.split(":");
							sortList.add(SortBuilders
									.geoDistanceSort(fieldname,
											new GeoPoint(Double.parseDouble(_geo[0]), Double.parseDouble(_geo[0])))
									.order(reverse ? SortOrder.DESC : SortOrder.ASC));
							break;
						}
						if ((checked = instanceConfig.getWriteField(fieldname)) != null) {
							sortList.add(SortBuilders.fieldSort(checked.getAlias())
									.order(reverse ? SortOrder.DESC : SortOrder.ASC));
						} else if ((sp = instanceConfig.getSearcherParam(fieldname)) != null) {
							String fields = sp.getFields();
							if (fields != null) {
								for (String k : fields.split(",")) {
									sortList.add(SortBuilders.fieldSort(k).order(reverse ? SortOrder.DESC : SortOrder.ASC));
								}
							}
						} else if (fieldname.equals(GlobalParam.DEFAULT_FIELD)) {
							sortList.add(SortBuilders.fieldSort(fieldname).order(reverse ? SortOrder.DESC : SortOrder.ASC));
						}
						break;
				}
			}
		}
		if (!useScore)
			sortList.add(SortBuilders.scoreSort().order(SortOrder.DESC));
		return sortList;
	}

	@Override
	public List<SortBuilder<?>> getSortinfo() {
		return sortinfo;
	}

	public void setSorts(List<SortBuilder<?>> sortinfo) {
		this.sortinfo = sortinfo;
	}   
}
