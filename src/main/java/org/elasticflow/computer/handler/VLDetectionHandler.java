package org.elasticflow.computer.handler;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
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
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.model.DataSetReader;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFHttpClientUtil;
import org.elasticflow.yarn.Resource;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject; 

/**
 * Customized detection of multi-modal large models
 * 
 * return data
 * {
 *   id:数据标识
 *   from:数据来源
 *   data:payload
 *   create_time:生成时间
 *   parsemap:数据字段解析方法
 * } 
 * @author chengwen
 * @version 0.1
 * @create_time 2023-2023-12-19
 */

public class VLDetectionHandler extends ComputerHandler {

	protected ConnectParams connectParams;
	
	//Blocking statistical judgment time length
	protected final static int delayTime = 200;//ms 
	
	@Override
	public void release() {
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleData(ComputerFlowSocket invokeObject, Context context, DataSetReader dataSetReader)
			throws EFException {
		RestComputer RS = (RestComputer) invokeObject;
		// Concurrency control signal
		AtomicBoolean successRunAll = new AtomicBoolean(true);
		ArrayBlockingQueue<String> apiBlockingQueue;

		RS.getDataPage().put(GlobalParam.READER_KEY, context.getInstanceConfig().getComputeParams().getKeyField());
		RS.getDataPage().put(GlobalParam.READER_SCAN_KEY,
				context.getInstanceConfig().getComputeParams().getScanField());
		JSONObject requstParams = context.getInstanceConfig().getComputeParams().getApiRequest();
		JSONObject responseParams = context.getInstanceConfig().getComputeParams().getApiResponse();

		CopyOnWriteArrayList<String> apis = context.getInstanceConfig().getComputeParams().getApi();
		if (apis.size() == 0)
			throw new EFException("no available api configured!", ELEVEL.BreakOff);
		apiBlockingQueue = new ArrayBlockingQueue<>(apis.size());
		for (String api : apis) {
			if (api.contains(GlobalParam.IP)) {
				// Localization optimization to improve efficiency
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
					Resource.threadPools.execute(() -> {
						JSONObject tmp = null;
						try {
							List<Object> dts = this.processPostUnitData(units, context, requstParams);
							JSONObject jo = (JSONObject) dts.get(0);
							if (jo.size() > 0) {
								long start = 0, start2 = 0;
								if (GlobalParam.DEBUG) {
									start = Common.getNow();
								} 
								tmp = this.sentRequest(jo, api);
								if (GlobalParam.DEBUG) {
									start2 = Common.getNow();
								}
								apiBlockingQueue.put(api);
								if (tmp.getInteger("status") != 0) {// response status check
									RS.flowStatistic.incrementCurrentTimeFailProcess(1);
									Common.LOG.warn(context.getInstanceConfig().getInstanceID() + " predict warn.");
									Common.LOG.warn(jo.toJSONString());
									Common.LOG.warn(tmp.toJSONString());
								} else {
									this.write(context, tmp,responseParams,(ArrayList<JSONObject>) dts.get(1), RS);
								}
								if (GlobalParam.DEBUG) {
									Common.LOG.info("{} use {}s, process {}s", api, start2 - start, Common.getNow() - start2);
								}
							} else {
								apiBlockingQueue.put(api);
							}
						} catch (Exception e) {
							successRunAll.set(false);
							Common.LOG.error(api);
							if (GlobalParam.DEBUG) {
								Common.LOG.warn(requstParams.toJSONString());
								if (tmp != null) {
									Common.LOG.warn(tmp.toJSONString());
								}
							}
							Common.LOG.error("{} rest post exception", api,e);
						} finally {
							taskSingal.countDown();
						}
					});
				} catch (Exception e) {
					successRunAll.set(false);
					taskSingal.countDown();
					Common.LOG.error("{} Api Blocking Queue Exception",apiBlockingQueue.element(), e);
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
				Resource.threadPools.execute(() -> {
					JSONObject tmp = null;
					try {
						// [post_data,temp_data,keepDatas]
						List<Object> dts = this.processPostUnitData(units, context, requstParams);
						JSONObject jo = (JSONObject) dts.get(0);
						if (jo.size() > 0) {
							tmp = this.sentRequest(jo, api);
							apiBlockingQueue.put(api);
							if (tmp.getInteger("status") != 0) {// response status check
								RS.flowStatistic.incrementCurrentTimeFailProcess(count.get());
								Common.LOG.warn(context.getInstanceConfig().getInstanceID() + " predict warn.");
								Common.LOG.warn(tmp.toJSONString());
								Common.LOG.warn(((JSONObject) dts.get(0)).toJSONString());
							} else {
								this.write(context, tmp, responseParams,
										(ArrayList<JSONObject>) dts.get(1), RS);
							}
						} else {
							apiBlockingQueue.put(api);
						}
					} catch (Exception e) {
						successRunAll.set(false);
						Common.LOG.error(api);
						if (GlobalParam.DEBUG) {
							Common.LOG.warn(requstParams.toJSONString());
							if (tmp != null) {
								Common.LOG.warn(tmp.toJSONString());
							}
						}
						Common.LOG.error("{} rest post data process error",api, e);
					} finally {
						taskSingal.countDown();
					}
				});
			} catch (Exception e) {
				successRunAll.set(false);
				taskSingal.countDown();
				Common.LOG.error("{} Api Blocking Queue Exception",apiBlockingQueue.element(), e);
			}
		}

		// fork threads
		try {
			taskSingal.await(90, TimeUnit.SECONDS);
		} catch (Exception e) {
			throw new EFException(e.getMessage(), ELEVEL.Ignore);
		}

		if (successRunAll.get() == false)
			throw new EFException("job executorService exception", ELEVEL.Ignore);
		// prepare result
		RS.getDataPage().put(GlobalParam.READER_LAST_STAMP, dataSetReader.getScanStamp());
		RS.getDataPage().putData(RS.getDataUnit());
		RS.getDataPage().putDataBoundary(dataSetReader.getDataBoundary());

	}
	
	
	/**
	 * process restful post Unit data
	 * @param units
	 * @param context
	 * @param requstParams
	 * @return  [post_data,temp_data,keepDatas]
	 * @throws EFException
	 */
		private List<Object> processPostUnitData(ArrayList<PipeDataUnit> units,Context context,JSONObject requstParams) throws EFException {
			JSONObject post_data = new JSONObject();  
			//Retain incoming data that needs to be further written downstream
			ArrayList<JSONObject> keepDatas = new ArrayList<>(); 
			boolean writeOneTime = false;
			for(PipeDataUnit pdu:units) { 
				JSONObject fieldParseMap = (JSONObject) pdu.getData().get("parsemap");			
				//process each field of data row
				Set<Entry<String, Object>> itr = requstParams.entrySet(); 
				for (Entry<String, Object> k : itr) {
					JSONObject fielddes = requstParams.getJSONObject(k.getKey());  
					if (fielddes.getString("type").equals("list")) {
						Object v = null;
						if(fielddes.containsKey("field")) {
							Queue<String> queue = new LinkedList<>(Arrays.asList(fielddes.getString("field").split("\\.")));
							String realField = ((LinkedList<String>) queue).getLast();
							v = this.getData(pdu.getData(), queue, fieldParseMap);
							v = this.parseValue(realField, v, fieldParseMap,true);
							if(v==null && fielddes.containsKey("required"))//check data field
								throw new EFException(k.getKey()+" required field can not obtain value from "+fielddes.containsKey("field"),ELEVEL.Dispose);
							if (!post_data.containsKey(k.getKey())) {
								post_data.put(k.getKey(), new JSONArray());  
							}	
							((JSONArray) post_data.get(k.getKey())).add(v); 
						}else if(fielddes.containsKey("values") && writeOneTime==false) {
							v = fielddes.getJSONArray("values"); 
							post_data.put(k.getKey(), v);  
							writeOneTime = true;
						}   
					} else {
						Common.LOG.error("post {} only support list!",fielddes.getString("type"));
					}
				} 
				//keep field data
				keepDatas.add(this.keepData(pdu.getData(),
						context.getInstanceConfig().getComputeParams().getCustomParams().getJSONArray("keepFields"),
						context.getInstanceConfig().getComputeFields()));
			}	
			List<Object> res = new ArrayList<>();
			res.add(post_data);  
			res.add(keepDatas); 
			return res; 
		}
		/**
		 * reflect parse value
		 * hdfs object
		 * @param field
		 * @param val
		 * @param fieldParseMap
		 * @return
		 * @throws EFException 
		 */
		private Object parseValue(String field, Object val, JSONObject fieldParseMap,boolean keepsource) throws EFException {
			if (fieldParseMap!=null && fieldParseMap.containsKey(field)) {
				try {
					Class<?> clz = Class.forName(fieldParseMap.getString(field));
					Method m;
					if(keepsource) {
						m = clz.getMethod("parseKeep", VLDetectionHandler.class, Object.class);
					}else {
						m = clz.getMethod("parse", VLDetectionHandler.class, Object.class);
					} 
					return m.invoke(null, this, val);
				} catch (Exception e) { 
					throw new EFException(e, ELEVEL.Dispose);
				}
			} 		
			return val;
		}
	/**
	 * Output data processing
	 * @param context
	 * @param datas
	 * @param sourcedt
	 * @param responseParams
	 * @param keepDatas
	 * @param RS
	 * @throws EFException
	 */
	private void write(Context context, JSONObject datas,JSONObject responseParams,
			ArrayList<JSONObject> keepDatas, RestComputer RS) throws EFException { 
		//response data field name
		String datafield = responseParams.getJSONObject("dataField").getString("name");				
		JSONArray JA = datas.getJSONArray(datafield); 
		if(JA.size()<1) { 
			Common.LOG.warn(responseParams.toJSONString());
			throw new EFException(context.getInstanceConfig().getInstanceID()+" unable to get data from response.",ELEVEL.BreakOff); 
		}	
		if(keepDatas!=null && JA.size()!=keepDatas.size())
			throw new EFException("The predicted result does not match the number of input data entries,response "+JA.size()+" request "+keepDatas.size()+".",ELEVEL.BreakOff); 
		

		// Tell the later build the fields and methods that need special processing
		JSONObject parsemap = new JSONObject();
		if (context.getInstanceConfig().getComputeFields().containsKey("parsemap")) {
			String parsemapstr = context.getInstanceConfig().getComputeFields().get("parsemap")
					.getDefaultvalue();
			if (parsemapstr != "") {
				parsemap.putAll(JSONObject.parseObject(parsemapstr));
			}
		}
		for (int i = 0; i < JA.size(); i++) { 
			JSONArray events = ((JSONObject) JA.get(i)).getJSONArray("events"); 
			int rownum = 0;
			if (events.size() > 0) {
				rownum = events.size();		
			}  	 
			this.unitProcess(i, rownum, responseParams, keepDatas, RS,events, context, datas, parsemap);
		}
	}
	
	private void unitProcess(int i,int rownum,JSONObject responseParams,ArrayList<JSONObject> keepDatas, RestComputer RS,
			JSONArray events,Context context,JSONObject sourcedatas,JSONObject parsemap) throws EFException { 
		try {
			for(int j=0;j<rownum;j++) {
				JSONObject row = new JSONObject();						 	
				JSONObject res = new JSONObject(); 
				res.putAll(keepDatas.get(i));  
				if (events.size() > 0) {
					res.put("events", events.get(j));
				} else {
					res.put("events", new JSONArray());
				}
				row.put("data", res);
				row.put("id",res.get("id"));				
				row.put("from", sourcedatas.getString("source"));
				row.put("create_time", Common.getNow());
				row.put("parsemap", parsemap);	
				PipeDataUnit u = PipeDataUnit.getInstance();
				u.setReaderKeyVal(res.get("id"));
				PipeDataUnit.addFieldValue("data", row, context.getInstanceConfig().getWriteFields(),u);
				RS.getDataUnit().add(u);
			} 	
		} catch (EFException e) {
			throw e;
		} 
	}
	 
	/**
	 * Round robin send request
	 * 
	 * @param post_data
	 * @return
	 * @throws EFException
	 */
	private JSONObject sentRequest(JSONObject post_data, String api) throws EFException {
		EFHttpResponse response = EFHttpClientUtil.process(api, post_data.toString());
		if (response.isSuccess()) {
			return JSONObject.parseObject(response.getPayload());
		} else {
			throw new EFException(response.getInfo(), ELEVEL.Dispose);
		}
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
			if (k.getKey().equals("data")) {
				if (keepfields!=null && keepfields.contains(k.getKey())) {
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
		}
		return dt;
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
	private Object getData(HashMap<String, Object> data, Queue<String> fields,
			JSONObject fieldParseMap) throws EFException {
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
}
