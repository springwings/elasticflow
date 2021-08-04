/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.reader.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.field.EFField;
import org.elasticflow.model.EFSearchRequest;
import org.elasticflow.model.EFSearchResponse;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.searcher.SearcherESModel;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.node.CPU;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.service.EFService;
import org.elasticflow.service.HttpService;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
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

	private EFService FS;

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
			response.setHeader("PowerBy", GlobalParam.PROJ);
			Request rq = (request instanceof Request) ? (Request) request
					: HttpConnection.getCurrentConnection().getRequest();

			try (BufferedReader _br = new BufferedReader(new InputStreamReader(rq.getInputStream(), "UTF-8"));) {
				String line = null;
				StringBuilder sb = new StringBuilder();
				while ((line = _br.readLine()) != null) {
					sb.append(line);
				}
				EFSearchRequest RR = Common.getRequest(sb.toString());
				EFSearchResponse rps = EFSearchResponse.getInstance();
				rps.setRequest(RR.getParams()); 
				RR.setPipe(rq.getPathInfo().substring(1)); 
				if (RR.getPipe().length() < 1) {
					rps.setStatus("The writer destination is empty!", RESPONSE_STATUS.ParameterErr);
				} else if (RR.getParam("ac") != null && RR.getParam("code") != null
						&& RR.getParam("code").equals(MD5Util.SaltMd5(RR.getPipe()))) {
					String ac = (String) RR.getParam("ac");
					switch (ac) {
					case "add":
						if (RR.getParam("data") != null && RR.getParam("instance") != null
								&& RR.getParam("type") != null && RR.getParam("keycolumn") != null
								&& RR.getParam("updatecolumn") != null) {
							String instance = (String) RR.getParam("instance");
							String seq = (String) RR.getParam("seq");
							String keycolumn = (String) RR.getParam("keycolumn");
							String updatecolumn = (String) RR.getParam("updatecolumn");
							boolean monopoly = false;
							if (RR.getParam("ef_is_monopoly") != null && RR.getParam("ef_is_monopoly").equals("true"))
								monopoly = true;

							PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(instance, seq, false,
									monopoly ? GlobalParam.FLOW_TAG._MOP.name() : GlobalParam.FLOW_TAG._DEFAULT.name());
							if (pipePump == null || !pipePump.getInstanceConfig().getAlias().equals(RR.getPipe())) {
								rps.setStatus("Writer get Error,Instance not exits!", RESPONSE_STATUS.DataErr);
								break;
							}
							String storeid;
							if (RR.getParam("type").equals("full") && RR.getParam("storeid") != null) {
								storeid = (String) RR.getParam("storeid");
							} else {
								storeid = Common.getStoreId(instance, seq, pipePump, true, false);
							}
							boolean isUpdate = false;

							if (RR.getParam("ef_is_update") != null && RR.getParam("ef_is_update").equals("true"))
								isUpdate = true;

							try {
								String writeTo = pipePump.getInstanceConfig().getPipeParams().getInstanceName();
								if (writeTo == null) {
									writeTo = Common.getMainName(instance, seq);
								} 
								DataPage pagedata = this.getPageData(RR.getParam("data"), keycolumn, updatecolumn,
										pipePump.getInstanceConfig().getWriteFields());
								if (pipePump.getInstanceConfig().openCompute()) {
									pagedata = (DataPage) CPU.RUN(pipePump.getID(), "ML", "compute", false, pipePump.getID(),"add",
											writeTo, pagedata); 				
								} 
								CPU.RUN(pipePump.getID(), "Pipe", "writeDataSet", false, "HTTP PUT", writeTo, storeid,
										"", pagedata,
										"", isUpdate, monopoly);
							} catch (Exception e) {
								Common.LOG.error("Http Write Exception,", e);
								rps.setStatus("Write failure data error!", RESPONSE_STATUS.DataErr);
								try {
									throw new EFException("写入参数错误，instance:" + instance + ",seq:" + seq);
								} catch (EFException fe) {
									e.printStackTrace();
								}
							}
						} else {
							rps.setStatus("parameters Not all set!", RESPONSE_STATUS.ParameterErr);
						}
						break;
					case "get_new_storeid":
						if (RR.getParam("instance") != null && RR.getParam("seq") != null) {
							PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(String.valueOf(RR.getParam("instance")),
									String.valueOf(RR.getParam("seq")), false, "");
							String storeid = Common.getStoreId(String.valueOf(RR.getParam("instance")), String.valueOf(RR.getParam("seq")), pipePump,
									false, false);
							CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true, RR.getParam("instance"),
									storeid);
						} else {
							rps.setStatus("Failed to create new index ID!", RESPONSE_STATUS.ExternErr);
						}
						break;
					case "switch":
						if (RR.getParam("instance") != null && RR.getParam("seq") != null) {
							String storeid;
							String instance = (String) RR.getParam("instance");
							String seq = (String) RR.getParam("seq");
							PipePump pipePump = Resource.SOCKET_CENTER.getPipePump(instance, seq, false,
									GlobalParam.FLOW_TAG._DEFAULT.name());
							if (RR.getParam("storeid") != null) {
								storeid = (String) RR.getParam("storeid");
							} else {
								storeid = Common.getStoreId(instance, seq, pipePump, false, false);
								CPU.RUN(pipePump.getID(), "Pond", "createStorePosition", true, instance, storeid);
							}
							CPU.RUN(pipePump.getID(), "Pond", "switchInstance", true, instance, seq, storeid);
							pipePump.run(instance, storeid, seq, true,
									pipePump.getInstanceConfig().getPipeParams().getInstanceName() == null ? false
											: true);
						} else {
							rps.setStatus("Failed to switch index!", RESPONSE_STATUS.ExternErr);
						}
						break;
					case "delete":
						if (RR.getParam("instance") != null && RR.getParam("seq") != null
								&& RR.getParam("search_dsl") != null) {
							SearcherModel<?, ?, ?> query = null;
							String instance = (String) RR.getParam("instance");
							String seq = (String) RR.getParam("seq");
							PipePump transFlow = Resource.SOCKET_CENTER.getPipePump(instance, seq, false,
									GlobalParam.FLOW_TAG._DEFAULT.name());
							if (transFlow == null) {
								rps.setStatus("Writer get Error,Instance and seq Error!", RESPONSE_STATUS.DataErr);
								break;
							}
							String storeid = Common.getStoreId(instance, seq, transFlow, true, true);
							WarehouseParam param = Resource.SOCKET_CENTER
									.getWHP(transFlow.getInstanceConfig().getPipeParams().getWriteTo());
							switch (param.getType()) {
							case ES:
								query = SearcherESModel.getInstance(Common.getRequest(rq),
										transFlow.getInstanceConfig());
								break;
							default:
								break;
							}
							CPU.RUN(transFlow.getID(), "Pond", "deleteByQuery", true, query, instance, storeid);
						} else {
							rps.setStatus("instance,seq,search_dsl not set!", RESPONSE_STATUS.ParameterErr);
						}
						break;

					default:
						rps.setStatus("action not exists!", RESPONSE_STATUS.ParameterErr);
						break;
					}

				} else {
					rps.setStatus("code, ac is empty OR code not match!", RESPONSE_STATUS.ParameterErr);
				}
				response.getWriter().println(rps.getResponse(true));
				response.getWriter().flush();
				response.getWriter().close();
			} catch (Exception e) {
				log.error("http Handle Exception", e);
			}
		}

		private DataPage getPageData(Object data, String keycolumn, String updatecolumn,
				Map<String, EFField> transParams) throws EFException {
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
						u.setReaderKeyVal(k.getValue());
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
