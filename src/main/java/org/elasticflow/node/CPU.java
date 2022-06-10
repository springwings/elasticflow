/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.instruction.Context;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.writer.WriterFlowSocket;

/**
 * Instructions Dispatch, RUN and Track Center
 * 
 * @author chengwen
 * @version 1.0
 */
public class CPU { 
	
	static volatile HashMap<String, Context> Contexts = new HashMap<>();
	
	public static void prepare(String runId,InstanceConfig instanceConfig,List<WriterFlowSocket> writer,
			ReaderFlowSocket reader,ComputerFlowSocket computer) { 
		Contexts.put(runId, Context.initContext(instanceConfig, writer,reader,computer));
	}
	
	public static Context getContext(String runId) {
		return Contexts.get(runId);
	}
 
	/**
	 * RUN Instruction processing unit
	 * @param runId tag for instruction context
	 * @param instructionSet 指令集名称
	 * @param instruction 执行指令
	 * @return
	 * @throws Exception
	 */
	public static Object RUN(String runId,String instructionsSet,String instruction,boolean runCheck,Object... args) throws EFException{ 
		Object rs=null;
		try {
			Class<?> clz = Class.forName("org.elasticflow.instruction.sets."+instructionsSet); 
			Method m = clz.getMethod(instruction, Context.class,Object[].class);  
			if(instructionsSet.equals("Track")) {
				Object[] argsNew = new Object[args.length+1];
				argsNew[args.length] = runId;
				System.arraycopy(args,0,argsNew,0,args.length);
				rs = m.invoke(null,null,argsNew);
			}else if(Contexts.containsKey(runId)) {
				rs = m.invoke(null,Contexts.get(runId),args);
			}else {
				Common.LOG.error("{} context is not exists!",runId);
			}
		}catch (Exception e) {
			Common.LOG.warn("instruction description:runId {},instructionsSet {}",runId,instructionsSet);
			throw Common.getException(e);
		}
		return rs;
	}
	
	public static String getUUID() {
		return UUID.randomUUID().toString().replace("-", "");
	} 
	
	public static void free(String ObjectId) {
		Contexts.remove(ObjectId);
	}
}
