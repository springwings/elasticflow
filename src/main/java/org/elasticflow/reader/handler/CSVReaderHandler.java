/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.reader.handler;

import java.io.RandomAccessFile;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.reader.flow.FilesReader;
import org.elasticflow.util.EFException;

/**
 * user defined read data process function
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-12-28 09:27
 */
public class CSVReaderHandler extends ReaderHandler {

	private String[] csvHeader;

	@Override
	public <T> T handlePage(Object invokeObject, Task task, int pageSize) throws EFException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void handleData(Object invokeObject, Object datas, Page page, int pageSize) throws EFException {
		FilesReader FR = (FilesReader) invokeObject;
		try {
			try (RandomAccessFile rf = new RandomAccessFile((String) datas, "r");) {
				int pos = 0;
				rf.seek(Integer.parseInt(page.getStart()));
				while (pos < pageSize) {
					if (page.getStart().equals("0") && pos == 0) {
						csvHeader = rf.readLine().split(",");
					} else {
						String line;
						while ((line = rf.readLine()) != null) {
							String[] row = line.split(",");
							PipeDataUnit u = PipeDataUnit.getInstance();
							for (int i = 0; i < csvHeader.length; i++) {
								PipeDataUnit.addFieldValue(csvHeader[i], row[i],
										page.getInstanceConfig().getReadFields(), u);
							}
							FR.getDataUnit().add(u);							
						}
					}
					pos++;
				}
			}
			FR.getDataPage().putData(FR.getDataUnit());
			FR.getDataPage().put(GlobalParam.READER_STATUS, true);
		} catch (Exception e) {
			throw new EFException(e);
		}
	}
}
