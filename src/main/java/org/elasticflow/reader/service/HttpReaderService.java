package org.elasticflow.reader.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.searcher.SearcherESModel;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.node.CPU;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.service.FNService;
import org.elasticflow.service.HttpService;
import org.elasticflow.util.Common;
import org.elasticflow.util.FNException;
import org.elasticflow.util.MD5Util;
import org.elasticflow.yarn.Resource;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * Reader open http port support data read
 * 
 * @author chengwen
 * @version 1.0
 */
public class HttpReaderService {
 
	private final static Logger log = LoggerFactory.getLogger(HttpReaderService.class);  

	private FNService FS;

	public boolean start() {
		HashMap<String, Object> serviceParams = new HashMap<String, Object>();
		serviceParams.put("confident_port", GlobalParam.StartConfig.get("reader_service_confident_port"));
		serviceParams.put("max_idle_time", GlobalParam.StartConfig.get("reader_service_max_idle_time"));
		serviceParams.put("port", GlobalParam.StartConfig.get("reader_service_port"));
		serviceParams.put("thread_pool", GlobalParam.StartConfig.get("reader_service_thread_pool"));
		serviceParams.put("httpHandle", new httpHandle());
		FS = HttpService.getInstance(serviceParams);
		FS.start();
		return true;
	}

	public boolean close() {
		if (FS != null) {
			FS.close();
		}
		return true;
	}

	public class httpHandle extends AbstractHandler {

		@Override
		public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) {
			response.setContentType("application/json;charset=utf8");
			response.setStatus(HttpServletResponse.SC_OK);
			response.setHeader("PowerBy", "rivers");  
			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();

			String dataTo = rq.getPathInfo().substring(1);
			try {
				if (dataTo.length() < 1) {
					response.getWriter().println("{\"status\":0,\"info\":\"The write destination is empty!\"}");
					response.getWriter().flush();
					response.getWriter().close();
					return;
				}
				if (rq.getParameterMap().get("ac") != null && rq.getParameterMap().get("code") != null
						&& rq.getParameter("code").equals(MD5Util.SaltMd5(dataTo))) {
					switch (rq.getParameter("ac")) {
					case "add":
						if (rq.getParameterMap().get("data") != null && rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("type") != null && rq.getParameterMap().get("seq") != null
								&& rq.getParameterMap().get("keycolumn") != null
								&& rq.getParameterMap().get("updatecolumn") != null) {
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							String keycolumn = rq.getParameter("keycolumn");
							String updatecolumn = rq.getParameter("updatecolumn");
							boolean monopoly = false;
							if (rq.getParameterMap().get("fn_is_monopoly") != null
									&& rq.getParameter("fn_is_monopoly").equals("true"))
								monopoly = true;
							
							PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(instance, seq, false,monopoly?GlobalParam.FLOW_TAG._MOP.name():GlobalParam.FLOW_TAG._DEFAULT.name());
							if (pipePump == null || !pipePump.getInstanceConfig().getAlias().equals(dataTo)) {
								response.getWriter().println("{\"status\":0,\"info\":\"Writer get Error,Instance not exits!\"}");
								break;
							}
							String storeid;
							if (rq.getParameter("type").equals("full") && rq.getParameterMap().get("storeid") != null) {
								storeid = rq.getParameter("storeid");
							} else {
								storeid = Common.getStoreId(instance, seq, pipePump, true, false);
							}
							boolean isUpdate = false; 
							
							if (rq.getParameterMap().get("fn_is_update") != null
									&& rq.getParameter("fn_is_update").equals("true"))
								isUpdate = true;  
							
							try {
								String writeTo = pipePump.getInstanceConfig().getPipeParams().getInstanceName();
								if(writeTo==null) {
									writeTo = Common.getMainName(instance, seq);
								}
								CPU.RUN(pipePump.getID(), "Pipe", "writeDataSet", false, "HTTP PUT",
										writeTo,
										storeid, "", getPageData(rq.getParameter("data"), keycolumn, updatecolumn,
												pipePump.getInstanceConfig().getWriteFields()),
										"", isUpdate,monopoly); 
								response.getWriter().println("{\"status\":1,\"info\":\"success\"}");
							} catch (Exception e) {
								Common.LOG.error("Http Write Exception,",e);
								response.getWriter().println("{\"status\":0,\"info\":\"写入失败!参数错误，instance:" + instance
										+ ",seq:" + seq + "\"}");
								try {
									throw new FNException("写入参数错误，instance:" + instance + ",seq:" + seq);  
								} catch (FNException fe) {
									e.printStackTrace();
								}
							}
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"参数没有全部设置!\"}");
						}
						break;
					case "get_new_storeid":
						if (rq.getParameterMap().get("instance") != null && rq.getParameterMap().get("seq") != null) {
							PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(rq.getParameter("instance"),
									rq.getParameter("seq"), false,"");
							String storeid = Common.getStoreId(rq.getParameter("instance"), rq.getParameter("seq"),
									pipePump, false, false);
							CPU.RUN(pipePump.getID(), "Pond", "createStorePosition",true,rq.getParameter("instance"), storeid);   
							response.getWriter()
									.println("{\"status\":1,\"info\":\"success\",\"storeid\":\"" + storeid + "\"}");
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"创建新索引ID失败!\"}");
						}
						break;
					case "switch":
						if (rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("seq") != null) {
							String storeid;
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(instance, seq, false,GlobalParam.FLOW_TAG._DEFAULT.name());
							if(rq.getParameterMap().get("storeid")!=null) {
								storeid = rq.getParameter("storeid");
							}else {
								storeid = Common.getStoreId(instance, seq,
										pipePump, false, false);
								CPU.RUN(pipePump.getID(), "Pond", "createStorePosition",true,instance, storeid);    
							} 
							CPU.RUN(pipePump.getID(), "Pond", "switchInstance",true, instance,seq,storeid);
							pipePump.run(instance, storeid, seq, true,pipePump.getInstanceConfig().getPipeParams().getInstanceName()==null?false:true);
							response.getWriter().println("{\"status\":1,\"info\":\"success\"}");
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"切换索引失败!\"}");
						}
						break;
					case "delete":
						if (rq.getParameterMap().get("instance") != null
								&& rq.getParameterMap().get("seq") != null && rq.getParameterMap().get("search_dsl") != null) { 
							SearcherModel<?, ?, ?> query=null;
							String instance = rq.getParameter("instance");
							String seq = rq.getParameter("seq");
							PipePump transFlow = Resource.SOCKET_CENTER.getPipePump(instance, seq, false,GlobalParam.FLOW_TAG._DEFAULT.name()); 
							if (transFlow == null) {
								response.getWriter().println("{\"status\":0,\"info\":\"Writer get Error,Instance and seq Error!\"}");
								break;
							}
							String storeid = Common.getStoreId(instance,seq, transFlow, true, true);
							WarehouseParam param = Resource.SOCKET_CENTER.getWHP(transFlow.getInstanceConfig().getPipeParams().getWriteTo());
							switch (param.getType()) {
							case ES:
								query = SearcherESModel.getInstance(Common.getRequest(rq),transFlow.getInstanceConfig());
								break; 
							default:
								break;
							}
							CPU.RUN(transFlow.getID(), "Pond", "deleteByQuery",true, query, instance, storeid);							 
							response.getWriter().println("{\"status\":1,\"info\":\"success\"}");
						} else {
							response.getWriter().println("{\"status\":0,\"info\":\"instance,seq,search_dsl not set!\"}");
						}
						break;

					default:
						response.getWriter().println("{\"status\":0,\"info\":\"action not exists!\"}");
						break;
					}

				} else {
					response.getWriter().println("{\"status\":0,\"info\":\"code is empty OR code not match!\"}");
				}
				response.getWriter().flush();
				response.getWriter().close();
			} catch (Exception e) {
				log.error("http Handle Exception", e);
			}
		}

		private DataPage getPageData(Object data, String keycolumn, String updatecolumn,
				Map<String, RiverField> transParams) {
			DataPage DP = new DataPage();
			LinkedList<PipeDataUnit> datas = new LinkedList<PipeDataUnit>();
			DP.put(GlobalParam.READER_KEY, keycolumn);
			DP.put(GlobalParam.READER_SCAN_KEY, updatecolumn);
			DP.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
			JSONArray jr = JSONArray.fromObject(data);
			String dataBoundary = null;
			String updateFieldValue = null;
			for (int j = 0; j < jr.size(); j++) {
				PipeDataUnit u = PipeDataUnit.getInstance();
				JSONObject jo = jr.getJSONObject(j);
				@SuppressWarnings("unchecked")
				Set<Entry<String, String>> itr = jo.entrySet();
				for (Entry<String, String> k : itr) {
					if (k.getKey().equals(DP.get(keycolumn))) {
						u.setKeyColumnVal(k.getValue());
						dataBoundary = String.valueOf(k.getValue());
					}
					if (k.getKey().equals(updatecolumn)) {
						updateFieldValue = String.valueOf(k.getValue());
					}
					u.addFieldValue(k.getKey(), k.getValue(), transParams);
				}
				datas.add(u);
			}
			if (updateFieldValue == null) {
				DP.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
			} else {
				DP.put(GlobalParam.READER_LAST_STAMP, updateFieldValue);
			}
			DP.putDataBoundary(dataBoundary); 
			DP.putData(datas);
			return DP;
		}
	}

}
