package org.elasticflow.notifier;

import org.apache.http.client.methods.HttpPut;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.EFHttpClientUtil;
import org.elasticflow.yarn.Resource;

import com.alibaba.fastjson.JSONObject;

/**
 * EF-Api Error Notifier
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 * @modify 2021-06-11 10:45
 */
public class EFApiNotifier implements EFNotify{

	@Override
	public boolean send(String subject, String content, boolean sync) {
		if(sync) {
			return this.sendSyncMode(subject, content);
		}else {
			Resource.ThreadPools.execute(() -> { 
				this.sendSyncMode(subject, content);
			});
		}
		return true;
	}
	
	public boolean sendSyncMode(String subject, String content) {
		JSONObject jO = new JSONObject();
		jO.put("subject", subject);
		jO.put("ip", GlobalParam.IP);
		jO.put("content", content);
		String response = EFHttpClientUtil.process(GlobalParam.SEND_API_ON,
				jO.toJSONString(), HttpPut.METHOD_NAME, EFHttpClientUtil.DEFAULT_CONTENT_TYPE, 3000);
		JSONObject jr = JSONObject.parseObject(response);
		if(jr.containsKey("status")) {
			if (Integer.valueOf(String.valueOf(jr.get("status"))) == 0)
				return true;
			return false;
		}
		return true;
	}

}
