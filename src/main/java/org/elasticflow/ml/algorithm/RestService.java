package org.elasticflow.ml.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.EFField;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.computer.SamplePoint;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFHttpClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Rest API Compute
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class RestService extends ComputerFlowSocket{
	    
    protected final static Logger log = LoggerFactory.getLogger("RestService");
    
	public static RestService getInstance(final ConnectParams connectParams) {
		RestService o = new RestService();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public boolean loadModel(Object datas) {
		return false;
	}

	@Override
	public Object predict(SamplePoint point) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataPage predict(Context context,DataSetReader DSR) {		
		if(this.computerHandler!=null) {
			this.computerHandler.handleData(this, context, DSR);
		}else {
			this.dataPage.put(GlobalParam.READER_KEY, context.getInstanceConfig().getComputeParams().getKeyField());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY, context.getInstanceConfig().getComputeParams().getScanField());
			JSONObject requstParams = context.getInstanceConfig().getComputeParams().getApiRequest();
			JSONObject responseParams = context.getInstanceConfig().getComputeParams().getApiResponse();
			JSONObject data = new JSONObject(); 
			ArrayList<JSONObject> keepDatas = new ArrayList<>();
			String[] apis = context.getInstanceConfig().getComputeParams().getApi();
			while (DSR.nextLine()) { 
				PipeDataUnit pdu = 	DSR.getLineData();
				keepDatas.add(this.keepData(pdu.getData(), context.getInstanceConfig().getWriteFields(),
						context.getInstanceConfig().getComputeFields()));
				Set<Entry<String, Object>> itr = requstParams.entrySet();				
				for (Entry<String, Object> k : itr) { 
					JSONObject fielddes = (JSONObject) k.getValue();
					Queue<String> queue = new LinkedList<>(Arrays.asList(fielddes.getString("field").split("\\.")));				
					if(fielddes.getString("type").equals("list")) {
						if(!data.containsKey(k.getKey())) {
							data.put(k.getKey(), new JSONArray());
						}
						JSONArray _JR = (JSONArray) data.get(k.getKey());
						_JR.add(this.getData(pdu.getData(), queue));
					}else {
						data.put(k.getKey(), this.getData(pdu.getData(), queue));
					}
				}
			} 
			JSONObject res = JSONObject.parseObject(EFHttpClientUtil.process(apis[0], data.toString()));
			JSONArray JA = res.getJSONArray(responseParams.getString("dataField"));
			
			String dataBoundary = null;
			String LAST_STAMP = null;
			for (int i = 0; i < JA.size(); i++) {
				JSONObject jr = keepDatas.get(i);
				jr.putAll((JSONObject) JA.get(i));
				Set<Entry<String, Object>> itr = jr.entrySet();	
				PipeDataUnit u = PipeDataUnit.getInstance();
				for (Entry<String, Object> k : itr) { 
					if(context.getInstanceConfig().getWriteFields().containsKey(k.getKey())) {
						u.addFieldValue(k.getKey(), k.getValue(),context.getInstanceConfig().getWriteFields());
						if (k.getKey().equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))) {
							LAST_STAMP = String.valueOf(k.getValue());
						}else if(k.getKey().equals(this.dataPage.get(GlobalParam.READER_KEY))) {
							u.setReaderKeyVal(k.getValue());
							dataBoundary = String.valueOf(k.getValue());
						}
					}
				}
				this.dataUnit.add(u);
			}  
			if (LAST_STAMP == null) {
				this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis());
			} else {
				this.dataPage.put(GlobalParam.READER_LAST_STAMP, LAST_STAMP);
			} 
			this.dataPage.putData(this.dataUnit);
			this.dataPage.putDataBoundary(dataBoundary);	
		}		  	
		return this.dataPage;
	}
	
	/**
	 * Traverse the store field and keep the value of writer field
	 * @param data
	 * @param transfields
	 * @param computeField
	 * @return
	 */
	private JSONObject keepData(HashMap<String,Object> data,Map<String, EFField> transfields,Map<String, EFField> computeField) {
		JSONObject dt = new JSONObject();
		Set<Entry<String, Object>> itr = data.entrySet();
		for (Entry<String, Object> k : itr) {
			EFField ef = computeField.get(k.getKey());
			if(transfields.containsKey(k.getKey())) {
				dt.put(k.getKey(), k.getValue());
			}		
		}
		return dt;
	}
	
	/**
	 * Extract the required value according to the request map
	 * @param data
	 * @param fields
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Object getData(HashMap<String,Object> data,Queue<String> fields) {
		Set<Entry<String, Object>> itr = data.entrySet();
		String field = fields.poll();
		Object rs = null;
		for (Entry<String, Object> k : itr) {
			if(k.getKey().equals(field)) {
				rs = k.getValue();
				break;
			}
		}
		if(fields.size()>0) {
			if(rs instanceof JSONObject) {
				return getData((JSONObject)rs, fields);
			}else {
				return getData((HashMap<String,Object>)rs, fields);
			}
		}else {
			return rs;
		}
	}
	
	private Object getData(JSONObject JO,Queue<String> fields) {
		Set<Entry<String, Object>> itr = JO.entrySet();
		String field = fields.poll();
		Object data = null;
		for (Entry<String, Object> k : itr) {
			if(k.getKey().equals(field)) {
				data = k.getValue();
				break;
			}
		}
		if(fields.size()>0) {
			return getData((JSONObject)data, fields);
		}else {
			return data;
		}
	}

	@Override
	public DataPage train(Context context, DataSetReader DSR, Map<String, EFField> transParam) {
		// TODO Auto-generated method stub
		return null;
	}
 
 
}
