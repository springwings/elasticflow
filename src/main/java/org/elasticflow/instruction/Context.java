/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction;

import java.util.List;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFWriterUtil;
import org.elasticflow.writer.WriterFlowSocket;
 
/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-01-22 
 * @modify 2019-01-22 11:24
 */
public class Context { 
	
	private InstanceConfig instanceConfig;
	
	private List<WriterFlowSocket> writer;
	
	private ReaderFlowSocket reader;
	
	private ComputerFlowSocket computer; 
	
	public static Context initContext(InstanceConfig instanceConfig,List<WriterFlowSocket> writer,
			ReaderFlowSocket reader,ComputerFlowSocket computer) {
		Context c = new Context();
		c.instanceConfig = instanceConfig;
		c.writer = writer;
		c.reader = reader;
		c.computer = computer; 
		return c;
	}

	public InstanceConfig getInstanceConfig() {
		return instanceConfig;
	} 
	
	public WriterFlowSocket getWriter() {
		return writer.get(EFWriterUtil.getWriterSocketIndex(this.instanceConfig,writer.size(),0));
	}
	
	public WriterFlowSocket getWriter(Long outTime) {
		return writer.get(EFWriterUtil.getWriterSocketIndex(this.instanceConfig,writer.size(),outTime));
	}

	public ReaderFlowSocket getReader() {
		return reader;
	} 
	
	public ComputerFlowSocket getComputer() {
		return computer;
	} 
	 
}
