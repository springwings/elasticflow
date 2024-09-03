/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.computer.handler;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.computer.flow.RestComputer;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.field.EFField;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.EFHttpResponse;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.reader.model.DataSetReader;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFHttpClientUtil;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Using API to call algorithms to generate paragraph summaries
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-12-28 09:27
 */
public class ParagraphSummaryHandler extends ComputerHandler {

	protected final static int delayTime = 200;// ms

	public ArrayBlockingQueue<String> apiBlockingQueue;

	public JSONObject requstParams;

	public JSONObject responseParams;

	private final static Logger log = LoggerFactory.getLogger(ParagraphSummaryHandler.class);

	@Override
	public void release() {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleData(ComputerFlowSocket invokeObject, Context context, DataSetReader dataSetReader)
			throws EFException {
		AtomicBoolean successRunAll = new AtomicBoolean(true);
		AtomicInteger errorTime = new AtomicInteger(0);
		requstParams = context.getInstanceConfig().getComputeParams().getApiRequest();
		responseParams = context.getInstanceConfig().getComputeParams().getApiResponse();
		RestComputer RS = (RestComputer) invokeObject;
		CopyOnWriteArrayList<String> apis = context.getInstanceConfig().getComputeParams().getApi();
		if (apis.size() == 0)
			throw new EFException("No available algorithm service rest api.", ELEVEL.BreakOff);
		apiBlockingQueue = new ArrayBlockingQueue<>(apis.size());
		for (String api : apis) {
			if (api.contains(GlobalParam.IP)) {
				api = api.replace(GlobalParam.IP, "127.0.0.1");
			}
			apiBlockingQueue.add(api);
		}
		int sourcePageNum = (int) Math.ceil((dataSetReader.getDataNums() + 0.)
				/ context.getInstanceConfig().getComputeParams().apiRequestMaxDatas());
		CountDownLatch taskSingal = new CountDownLatch(sourcePageNum);

		// construct rest post data
		AtomicInteger count = new AtomicInteger(0);
		ArrayList<PipeDataUnit> pdus = new ArrayList<>();
		while (dataSetReader.nextLine()) {
			PipeDataUnit pdu = dataSetReader.getLineData();
			pdus.add(pdu);
			count.incrementAndGet();
			if (count.get() >= context.getInstanceConfig().getComputeParams().apiRequestMaxDatas()) {
				ArrayList<PipeDataUnit> units = (ArrayList<PipeDataUnit>) pdus.clone();
				try {
					long startTime = System.currentTimeMillis();
					String api = apiBlockingQueue.take();
					if (System.currentTimeMillis() - startTime > delayTime)
						RS.flowStatistic.incrementBlockTime();
					this.run(RS, units, context, api, count, successRunAll, errorTime, taskSingal);
				} catch (Exception e) {
					successRunAll.set(false);
					errorTime.getAndIncrement();
					taskSingal.countDown();
					log.error("Api Blocking Queue Exception", e);
				} finally {
					count.set(0);
					pdus.clear();
				}
			}
		}
		if (count.get() > 0) {
			ArrayList<PipeDataUnit> units = pdus;
			try {
				long startTime = System.currentTimeMillis();
				String api = apiBlockingQueue.take();
				if (System.currentTimeMillis() - startTime > delayTime)
					RS.flowStatistic.incrementBlockTime();
				this.run(RS, units, context, api, count, successRunAll, errorTime, taskSingal);
			} catch (Exception e) {
				successRunAll.set(false);
				errorTime.getAndIncrement();
				taskSingal.countDown();
				log.error("Api Blocking Queue Exception", e);
			}
		}
		// fork threads
		try {
			taskSingal.await(90, TimeUnit.SECONDS);
		} catch (Exception e) {
			throw new EFException(e.getMessage(), ELEVEL.Ignore);
		}

		if (successRunAll.get() == false)
			throw new EFException(
					context.getInstanceConfig().getInstanceID() + " job exception,fail times " + errorTime.get(),
					ELEVEL.Ignore);
		RS.getDataPage().put(GlobalParam.READER_LAST_STAMP, dataSetReader.getScanStamp());
		RS.getDataPage().putData(RS.getDataUnit());
		RS.getDataPage().putDataBoundary(dataSetReader.getDataBoundary());
	}

	@SuppressWarnings("unchecked")
	private void run(RestComputer RS, ArrayList<PipeDataUnit> units, Context context, String api, AtomicInteger count,
			AtomicBoolean successRunAll, AtomicInteger errorTime, CountDownLatch taskSingal) {
		Resource.threadPools.execute(() -> {
			JSONObject tmp = null;
			try {
				List<Object> dts = this.processUnitData(units, context, requstParams);
				JSONObject postObjects = (JSONObject) dts.get(0);
				if (postObjects.size() > 0) {
					long start = 0, start2 = 0;
					if (GlobalParam.DEBUG)
						start = Common.getNow();
					tmp = this.sentRequest(postObjects, api);
					if (GlobalParam.DEBUG)
						start2 = Common.getNow();
					apiBlockingQueue.put(api);
					if (tmp.getInteger("status") != 0) {// response status check
						RS.flowStatistic.incrementCurrentTimeFailProcess(count.get());
						log.warn(context.getInstanceConfig().getInstanceID() + " predict warn.");
						log.warn(postObjects.toJSONString());
						log.warn(tmp.toJSONString());
					} else {
						this.write(context, tmp, (JSONObject) dts.get(1), responseParams,
								(ArrayList<JSONObject>) dts.get(2), RS);
					}
					if (GlobalParam.DEBUG)
						log.info("{} use {}s, process {}s", api, start2 - start, Common.getNow() - start2);
				} else {
					apiBlockingQueue.put(api);
				}
			} catch (Exception e) {
				successRunAll.set(false);
				log.error(api);
				if (GlobalParam.DEBUG) {
					log.warn(requstParams.toJSONString());
					if (tmp != null)
						log.warn(tmp.toJSONString());
				}
				log.error(api);
				log.error("{} rest api request error!", context.getInstanceConfig().getInstanceID(), e);
				errorTime.getAndIncrement();
			} finally {
				taskSingal.countDown();
			}
		});
	}

	private void write(Context context, JSONObject datas, JSONObject sourcedt, JSONObject responseParams,
			ArrayList<JSONObject> keepDatas, RestComputer RS) throws EFException {
		// response data field name
		String datafield = responseParams.getJSONObject("dataField").getString("name");
		JSONArray JA = datas.getJSONArray(datafield);
		if (JA.size() < 1) {
			log.warn(responseParams.toJSONString());
			throw new EFException(context.getInstanceConfig().getInstanceID() + " unable to get data from response.",
					ELEVEL.BreakOff);
		}
		if (keepDatas != null && JA.size() != keepDatas.size())
			throw new EFException("The predicted result does not match the number of input data entries,response "
					+ JA.size() + " request " + keepDatas.size() + ".", ELEVEL.BreakOff);

		// Tell the later build the fields and methods that need special processing
		JSONObject parsemap = new JSONObject();
		if (context.getInstanceConfig().getComputeFields().containsKey("parsemap")) {
			String parsemapstr = context.getInstanceConfig().getComputeFields().get("parsemap").getDefaultvalue();
			if (parsemapstr != "") {
				parsemap.putAll(JSONObject.parseObject(parsemapstr));
			}
		}
		for (int i = 0; i < JA.size(); i++) {
			JSONArray summarys = ((JSONObject) JA.get(i)).getJSONArray("summarys");
			JSONObject row = keepDatas.get(i);
			JSONArray sums = new JSONArray();
			if (summarys.size() > 0) {				
				for(int j=0;j<summarys.size();j++) {
					JSONObject _row = new JSONObject();
					_row.put("id", row.getIntValue("id")*1000+j);
					_row.put("clue", summarys.get(j));
					sums.add(_row);
				}
			}
			row.put("summarys", sums); 
			PipeDataUnit u = PipeDataUnit.getInstance();
			u.setReaderKeyVal(row.get("id")); 
			for(String key : context.getInstanceConfig().getComputeFields().keySet()) {
				PipeDataUnit.addFieldValue(key, row.get(key), context.getInstanceConfig().getComputeFields(), u,true);
			} 
			RS.getDataUnit().add(u); 
		}
	}  

	/**
	 * process restful post data
	 * 
	 * @param units
	 * @param context
	 * @param requstParams
	 * @return
	 * @throws EFException
	 */
	private List<Object> processUnitData(ArrayList<PipeDataUnit> units, Context context, JSONObject requstParams)
			throws EFException {
		JSONObject post_data = new JSONObject();
		JSONObject temp_data = new JSONObject();
		ArrayList<JSONObject> keepDatas = new ArrayList<>();
		boolean addData = true;
		for (PipeDataUnit pdu : units) {
			addData = true;
			JSONObject fieldParseMap = (JSONObject) pdu.getData().get("parsemap");
			// process each field of data row
			Set<Entry<String, Object>> itr = requstParams.entrySet();
			for (Entry<String, Object> k : itr) {
				JSONObject fielddes = requstParams.getJSONObject(k.getKey());
				Queue<String> queue = new LinkedList<>(Arrays.asList(fielddes.getString("field").split("\\.")));
				if (fielddes.getString("type").equals("list")) {
					String realField = ((LinkedList<String>) queue).getLast();
					Object v = this.getData(pdu.getData(), queue, fieldParseMap);
					v = this.parseValue(realField, v, fieldParseMap, true);
					if (v != null) {// check data field
						if (!post_data.containsKey(k.getKey())) {
							post_data.put(k.getKey(), new JSONArray());
							temp_data.put(k.getKey(), new JSONArray());
						}
						((JSONArray) post_data.get(k.getKey())).add(v);
						((JSONArray) temp_data.get(k.getKey())).add(v);
					} else {
						addData = false;
						break;
					}
				} else {
					log.error("post {} only support list!", fielddes.getString("type"));
				}
			}
			if (addData) {// keep field data
				keepDatas.add(this.keepData(pdu.getData(),
						context.getInstanceConfig().getComputeParams().getCustomParams().getJSONArray("keepFields"),
						context.getInstanceConfig().getComputeFields()));
			}
		}
		List<Object> res = new ArrayList<>();
		res.add(post_data);
		res.add(temp_data);
		res.add(keepDatas);
		return res;
	}

	/**
	 * Traverse the store field and keep the value of writer field
	 * 
	 * @param data
	 * @param transfields
	 * @param computeField
	 * @return
	 */
	private JSONObject keepData(HashMap<String, Object> data, JSONArray keepfields, Map<String, EFField> computeField) {
		JSONObject dt = new JSONObject();
		Set<Entry<String, Object>> itr = data.entrySet();
		for (Entry<String, Object> k : itr) {
			if (keepfields != null && keepfields.contains(k.getKey())) {
				dt.put(k.getKey(), k.getValue());
			} else {
				Set<Entry<String, Object>> itr2 = null;
				if (k.getValue() instanceof JSONObject) {
					itr2 = ((JSONObject) k.getValue()).entrySet();
				} else if (k.getValue() instanceof HashMap) {
					itr2 = ((JSONObject) k.getValue()).entrySet();
				}
				if (itr2 != null) {
					for (Entry<String, Object> k2 : itr2) {
						if (keepfields.contains(k2.getKey())) {
							dt.put(k2.getKey(), k2.getValue());
						}
					}
				}
			}
		}
		return dt;
	}

	/**
	 * Round robin send request
	 * 
	 * @param post_data
	 * @return
	 * @throws EFException
	 */
	private JSONObject sentRequest(JSONObject post_data, String api) throws EFException {
		EFHttpResponse response = EFHttpClientUtil.process(api, post_data.toString(),500);
		try {
			if (response.isSuccess()) {
				return JSONObject.parseObject(response.getPayload());
			} else {
				throw new EFException(response.getInfo(), ELEVEL.Dispose);
			}
		} catch (Exception e) {
			log.error("send request exception",e);
			log.error(post_data.toString());
			log.error(response.getPayload());  
			throw new EFException(response.getInfo(), ELEVEL.Dispose);
		}
	}

	/**
	 * Extract the required value according to the request map
	 * 
	 * @param data
	 * @param fields
	 * @return
	 * @throws EFException
	 */
	@SuppressWarnings("unchecked")
	private Object getData(HashMap<String, Object> data, Queue<String> fields, JSONObject fieldParseMap)
			throws EFException {
		Set<Entry<String, Object>> itr = data.entrySet();
		String field = fields.poll();
		Object rs = null;
		for (Entry<String, Object> k : itr) {
			if (k.getKey().equals(field)) {
				rs = k.getValue();
				break;
			}
		}
		if (fields.size() > 0) {
			if (rs instanceof JSONObject) {
				return getData((JSONObject) rs, fields, fieldParseMap);
			} else {
				return getData((HashMap<String, Object>) rs, fields, fieldParseMap);
			}
		} else {
			return rs;
		}
	}

	/**
	 * Extract the required value according to the request map
	 * 
	 * @param data
	 * @param fields
	 * @return
	 * @throws EFException
	 */
	private Object getData(JSONObject JO, Queue<String> fields, JSONObject fieldParseMap) throws EFException {
		Set<Entry<String, Object>> itr = JO.entrySet();
		String field = fields.poll();
		Object data = null;
		for (Entry<String, Object> k : itr) {
			if (k.getKey().equals(field)) {
				data = k.getValue();
				break;
			}
		}
		if (fields.size() > 0) {
			return getData((JSONObject) data, fields, fieldParseMap);
		} else {
			return data;
		}
	}

	/**
	 * reflect parse value hdfs object
	 * 
	 * @param field
	 * @param val
	 * @param fieldParseMap
	 * @return
	 * @throws EFException
	 */
	private Object parseValue(String field, Object val, JSONObject fieldParseMap, boolean keepsource)
			throws EFException {
		if (fieldParseMap != null && fieldParseMap.containsKey(field)) {
			try {
				Class<?> clz = Class.forName(fieldParseMap.getString(field));
				Method m;
				if (keepsource) {
					m = clz.getMethod("parseKeep", ParagraphSummaryHandler.class, Object.class);
				} else {
					m = clz.getMethod("parse", ParagraphSummaryHandler.class, Object.class);
				}
				return m.invoke(null, this, val);
			} catch (Exception e) {
				throw new EFException(e, ELEVEL.Dispose);
			}
		}
		return val;
	}

}
