package org.elasticflow.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.EFField;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFHttpClientUtil;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Rest API Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class RestService extends ComputerFlowSocket {

	protected final static Logger log = LoggerFactory.getLogger("RestService");
	protected ArrayBlockingQueue<String> apiBlockingQueue;
	protected boolean successRunAll = true;

	public static RestService getInstance(final ConnectParams connectParams) {
		RestService o = new RestService();
		o.initConn(connectParams);
		return o;
	}

	@Override
	public DataPage predict(Context context, DataSetReader DSR) throws EFException {
		if (this.computerHandler != null) {
			this.computerHandler.handleData(this, context, DSR);
		} else {
			this.dataPage.put(GlobalParam.READER_KEY, context.getInstanceConfig().getComputeParams().getKeyField());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY,
					context.getInstanceConfig().getComputeParams().getScanField());
			JSONObject requstParams = context.getInstanceConfig().getComputeParams().getApiRequest();
			JSONObject responseParams = context.getInstanceConfig().getComputeParams().getApiResponse();
			
			CountDownLatch taskSingal = new CountDownLatch((int) Math.ceil((DSR.getDataNums()+0.)/
					context.getInstanceConfig().getComputeParams().apiRequestMaxDatas()));
			
			// construct thread pool
			CopyOnWriteArrayList<String> apis = context.getInstanceConfig().getComputeParams().getApi();
			this.apiBlockingQueue = new ArrayBlockingQueue<>(apis.size());
			for (String api : apis)
				this.apiBlockingQueue.add(api);

			// construct rest post data
			JSONObject post_data = new JSONObject();
			this.successRunAll = true;
			ArrayList<JSONObject> keepDatas = new ArrayList<>();
			int count = 0;

			while (DSR.nextLine()) {
				PipeDataUnit pdu = DSR.getLineData();
				keepDatas.add(this.keepData(pdu.getData(), context.getInstanceConfig().getComputeFields()));
				Set<Entry<String, Object>> itr = requstParams.entrySet();
				for (Entry<String, Object> k : itr) {
					JSONObject fielddes = (JSONObject) k.getValue();
					Queue<String> queue = new LinkedList<>(Arrays.asList(fielddes.getString("field").split("\\.")));
					if (fielddes.getString("type").equals("list")) {
						if (!post_data.containsKey(k.getKey())) {
							post_data.put(k.getKey(), new JSONArray());
						}
						((JSONArray) post_data.get(k.getKey())).add(this.getData(pdu.getData(), queue));
					} else {
						post_data.put(k.getKey(), this.getData(pdu.getData(), queue));
					}
				}
				count++;
				if (count >= context.getInstanceConfig().getComputeParams().apiRequestMaxDatas()) {
					JSONObject _postdt = (JSONObject) post_data.clone();
					@SuppressWarnings("unchecked")
					ArrayList<JSONObject> _keepdt = (ArrayList<JSONObject>) keepDatas.clone();					 
					try {
						if(this.apiBlockingQueue.isEmpty())
							this.flowState.incrementBlockTime();
						String api = this.apiBlockingQueue.take();
						Resource.threadPools.execute(() -> {						
							JSONObject tmp = null;	
							try {
								tmp = JSONObject.parseObject(this.sentRequest(_postdt, api));
								this.apiBlockingQueue.put(api);
								this.write(context, tmp, responseParams, _keepdt); 
							} catch (Exception e) { 
								this.successRunAll = false;
								log.error(apis.get(0));
								log.error("rest post data process error", e);
							}
							taskSingal.countDown();
			            }); 
					} catch (Exception e) { 
						this.successRunAll = false;
						log.error(apis.get(0));
						log.error("rest post data process error", e);
					}
					post_data.clear();
					keepDatas.clear();
					count = 0;
				}
			}

			if (count > 0) {
				JSONObject _postdt = (JSONObject) post_data.clone();
				@SuppressWarnings("unchecked")
				ArrayList<JSONObject> _keepdt = (ArrayList<JSONObject>) keepDatas.clone();
				try {
					if(this.apiBlockingQueue.isEmpty())
						this.flowState.incrementBlockTime();
					String api = this.apiBlockingQueue.take();
					Resource.threadPools.execute(() -> {						
						try {
							JSONObject tmp = null;	
							tmp = JSONObject.parseObject(this.sentRequest(_postdt, api));
							this.apiBlockingQueue.put(api);
							this.write(context, tmp, responseParams, _keepdt); 
						} catch (Exception e) { 
							this.successRunAll = false;
							log.error(apis.get(0));
							log.error("rest post data process error", e);
						}
						taskSingal.countDown();
		            }); 
				} catch (Exception e) { 
					this.successRunAll = false;
					log.error(apis.get(0));
					log.error("rest post data process error", e);
				}
				post_data.clear();
				keepDatas.clear();
			}
			
			try {
				if (this.successRunAll == true)
					taskSingal.await();
			} catch (Exception e) { 
				throw new EFException(e.getMessage(), ELEVEL.Ignore);
			}	
			
			if (this.successRunAll == false)
				throw new EFException("job executorService exception", ELEVEL.Termination);

			this.dataPage.put(GlobalParam.READER_LAST_STAMP, DSR.getScanStamp());
			this.dataPage.putData(this.dataUnit);
			this.dataPage.putDataBoundary(DSR.getDataBoundary());
		} 
		return this.dataPage;
	}

	/**
	 * Round robin send request
	 * 
	 * @param post_data
	 * @return
	 */
	private String sentRequest(JSONObject post_data, String api) {
		return EFHttpClientUtil.process(api, post_data.toString());
	}

	private void write(Context context, JSONObject datas, JSONObject responseParams, ArrayList<JSONObject> keepDatas)
			throws EFException {
		String datafield = responseParams.getJSONObject("dataField").getString("name");
		JSONArray JA = datas.getJSONArray(datafield);
		if (keepDatas != null && JA.size() != keepDatas.size())
			throw new EFException("predict result exception,"+datas.toString(), ELEVEL.Dispose);
		for (int i = 0; i < JA.size(); i++) {
			JSONObject jr = keepDatas.get(i);
			jr.putAll((JSONObject) JA.get(i));
			Set<Entry<String, Object>> itr = jr.entrySet();
			PipeDataUnit u = PipeDataUnit.getInstance();
			for (Entry<String, Object> k : itr) {
				PipeDataUnit.addFieldValue(k.getKey(), k.getValue(), context.getInstanceConfig().getComputeFields(), u);
				if (context.getInstanceConfig().getReadParams().getKeyField().equals(k.getKey())) {
					u.setReaderKeyVal(u.getData().get(k.getKey()));
				}
			}
			this.dataUnit.add(u);
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
	private JSONObject keepData(HashMap<String, Object> data, Map<String, EFField> transfields) {
		JSONObject dt = new JSONObject();
		Set<Entry<String, Object>> itr = data.entrySet();
		for (Entry<String, Object> k : itr) {
			if (transfields.size() == 0 || transfields.containsKey(k.getKey())) {
				dt.put(k.getKey(), k.getValue());
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
	 */
	@SuppressWarnings("unchecked")
	private Object getData(HashMap<String, Object> data, Queue<String> fields) {
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
				return getData((JSONObject) rs, fields);
			} else {
				return getData((HashMap<String, Object>) rs, fields);
			}
		} else {
			return rs;
		}
	}

	private Object getData(JSONObject JO, Queue<String> fields) {
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
			return getData((JSONObject) data, fields);
		} else {
			return data;
		}
	}
}
