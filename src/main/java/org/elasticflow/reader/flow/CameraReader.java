/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.reader.flow;

import java.awt.image.BufferedImage;
import java.net.URL;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.concurrent.NotThreadSafe;
import javax.imageio.ImageIO;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
 

/**
 * CameraReader Only supports pulling images directly from the camera
 * http://{}:{}@{}/ISAPI/Streaming/channels/1/picture
 * 
 * 
 * NotThreadSafe 
 * When starting multi-channel concurrency, careful consideration should be given to whether bugs will be introduced
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */

@NotThreadSafe
public class CameraReader extends ReaderFlowSocket { 

	public static CameraReader getInstance(final ConnectParams connectParams) {
		CameraReader o = new CameraReader();
		o.initConn(connectParams);
		return o;
	} 
 
	@Override
	public DataPage getPageData(final TaskCursor taskCursor, int pageSize) throws EFException {		
		try {
			this.dataPage.put(GlobalParam.READER_KEY, taskCursor.getReaderKey());
			this.dataPage.put(GlobalParam.READER_SCAN_KEY, taskCursor.getReaderScanKey());
			int count = 0; 
			JSONArray cameras = taskCursor.getInstanceConfig().getReaderParams().getCustomParams().getJSONArray("cameras");
			if (this.readHandler == null && this.readHandler.supportHandleData()) {
				for (int i = 0; i < cameras.size(); i++) {
					count++;
					if (count >= Integer.valueOf(taskCursor.getStart()) && count < Integer.valueOf(taskCursor.getEnd())) {
						JSONObject camera = cameras.getJSONObject(i);
						PipeDataUnit u = PipeDataUnit.getInstance(); 
						PipeDataUnit.addFieldValue("id",camera.getString("id"),
								taskCursor.getInstanceConfig().getReadFields(), u);
						PipeDataUnit.addFieldValue("pic_data", this.downImage(camera.getString("url")),
								taskCursor.getInstanceConfig().getReadFields(), u);
						PipeDataUnit.addFieldValue("create_time",Common.getNow(),
								taskCursor.getInstanceConfig().getReadFields(), u);
						this.dataUnit.add(u);
					}
				}
			} else {
				// handler reference mysql flow getAllData function
				this.readHandler.handleData(this,cameras, taskCursor, pageSize);
			}
		} catch (Exception e) { 
			this.dataPage.put(GlobalParam.READER_STATUS, false);  
			throw  new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " Kafka Reader get dataPage Exception");
		}
		return this.dataPage;
	}
	
	private BufferedImage downImage(String imageUrl) throws Exception {
		URL url = new URL(imageUrl);
        return ImageIO.read(url);
	}

	@Override
	public void flush() throws EFException {
		
	}

	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task, int pageSize) throws EFException { 
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		try {
			int totalNum = task.getInstanceConfig().getReaderParams().getCustomParams().getJSONArray("cameras").size();
			if (totalNum > 0) {
				int pagenum = (int) Math.ceil((totalNum + 0.) / pageSize);
				int curentpage = 0;
				while (true) {
					curentpage++;
					page.add(String.valueOf(curentpage * pageSize));
					if (curentpage >= pagenum)
						break;
				}
			}
		} catch (Exception e) { 
			page.clear();
			releaseConn(false, true);
			try {
				this.initFlow();
			} catch (EFException e1) {
				throw e1;
			}  
			throw new EFException(e,task.getInstanceID()+ " Camera Reader get page lists Exception!");
		}
		return page;
	}

}