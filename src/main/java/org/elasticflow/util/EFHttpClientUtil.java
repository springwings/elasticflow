/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticflow.config.GlobalParam.RESPONSE_STATUS;
import org.elasticflow.model.EFHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http Client Util
 * 
 * @author chengwen
 * @version 1.0
 * @date 2019-01-15 11:07
 * @modify 2019-01-15 11:07
 */
public class EFHttpClientUtil {

	public static PoolingHttpClientConnectionManager connectionPools;
	public static CloseableHttpClient httpClient;
	public static final String DEFAULT_CONTENT_TYPE = "application/json;charset=UTF-8";
	private static final int DEFAUL_TIME_OUT = 90; //second
	private static final int count = 10;
	private static final int totalCount = 1200;
	private static final int Http_Default_Keep_Time = 180; //second
	private static Logger logger = LoggerFactory.getLogger(EFHttpClientUtil.class);

	static {
		connectionPools = new PoolingHttpClientConnectionManager();
		connectionPools.setDefaultMaxPerRoute(count);
		connectionPools.setMaxTotal(totalCount);
		httpClient = HttpClients.custom().setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {
			public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
				HeaderElementIterator it = new BasicHeaderElementIterator(
						response.headerIterator(HTTP.CONN_KEEP_ALIVE)); 
				while (it.hasNext()) {
					HeaderElement he = it.nextElement();
					String param = he.getName();
					String value = he.getValue();
					if (value != null && param.equalsIgnoreCase("timeout")) {
						try {
							return Long.parseLong(value) * 1000;
						} catch (Exception e) {
							e.printStackTrace();
							logger.error("format KeepAlive timeout exception, exception:" + e.toString());
						}
					}
				}
				return Http_Default_Keep_Time * 1000L;
			}
		}).setConnectionManager(connectionPools).build();
	}

	/**
	 * Content-Type：application/json，Accept：application/json is used by default when
	 * executing HTTP post requests
	 * 
	 * @param uri		Request-URI
	 * @param data		request data
	 * @param timeout	second
	 * @return
	 */
	public static EFHttpResponse process(String uri, String data,int timeout) {
		return process(uri, data, HttpPost.METHOD_NAME, DEFAULT_CONTENT_TYPE, timeout, true);
	}
	
	public static EFHttpResponse process(String uri, String data) {
		return process(uri, data, HttpPost.METHOD_NAME, DEFAULT_CONTENT_TYPE, DEFAUL_TIME_OUT, true);
	}

	public static EFHttpResponse process(String uri, String methodName, String contentType, String data) {
		return process(uri, data, methodName, contentType, DEFAUL_TIME_OUT, true);
	}

	public static EFHttpResponse process(String uri, String methodName, String contentType) {
		return process(uri, null, methodName, contentType, DEFAUL_TIME_OUT, true);
	}

	public static EFHttpResponse process(String uri, String data, String methodName, String contentType, int timeout,
			boolean retry) {
		long startTime = System.currentTimeMillis();
		HttpRequestBase method = null;
		HttpEntity httpEntity = null;
		EFHttpResponse response = EFHttpResponse.getInstance();
		try {
			CloseableHttpResponse rps;
			method = getRequest(uri, methodName, contentType, timeout);
			if (data != null) {
				HttpEntityEnclosingRequestBase rq = (HttpEntityEnclosingRequestBase) method;
				rq.setEntity(new StringEntity(data, "UTF-8"));
				HttpContext context = HttpClientContext.create();
				rps = httpClient.execute(rq, context);
			} else {
				rps = httpClient.execute(getRequest(uri, methodName, contentType, timeout));
			}
			httpEntity = rps.getEntity();
			if (httpEntity != null) {
				response.setPayload(EntityUtils.toString(httpEntity, "UTF-8"));
				response.setUsems(System.currentTimeMillis() - startTime);
			}
		} catch (Exception e) {
			if (method != null) {
				method.abort();
			}
			if (e instanceof IOException && retry) {
				return EFHttpClientUtil.process(uri, data, methodName, contentType, timeout, false);
			}
			response.setStatus(RESPONSE_STATUS.ExternErr);
			response.setInfo(e.toString());
		} finally {
			if (httpEntity != null) {
				try {
					EntityUtils.consumeQuietly(httpEntity);
				} catch (Exception e2) {
					logger.error("close response exception, url:" + uri + ", exception:" + e2.toString()
							+ ", cost time(ms):" + (System.currentTimeMillis() - startTime));
				}
			}
		}
		return response;
	}

	/**
	 * Create request
	 * 
	 * @param uri
	 * @param methodName
	 * @param contentType
	 * @param timeout second
	 * @return
	 */
	private static HttpRequestBase getRequest(String uri, String methodName, String contentType, int timeout) {
		HttpRequestBase method = null;
		if (timeout <= 0) 
			timeout = DEFAUL_TIME_OUT;
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout * 1000)
				.setConnectTimeout(timeout * 1000).setConnectionRequestTimeout(timeout * 1000)
				.setExpectContinueEnabled(false).build();
		switch (methodName) {
		case HttpPut.METHOD_NAME:
			method = new HttpPut(uri);
			break;
		case HttpDelete.METHOD_NAME:
			method = new HttpDelete(uri);
			break;
		case HttpGet.METHOD_NAME:
			method = new HttpGet(uri);
			break;
		default:
			method = new HttpPost(uri);
			break;
		}
		if (StringUtils.isBlank(contentType)) {
			contentType = DEFAULT_CONTENT_TYPE;
		}
		method.addHeader("Content-Type", contentType);
		method.setConfig(requestConfig);
		return method;
	}
 
}