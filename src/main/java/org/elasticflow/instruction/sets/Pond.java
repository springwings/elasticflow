/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Pond extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Pond");

	/**
	 * @param args
	 *            parameter order is: String mainName,String storeId
	 */
	public static boolean createStorePosition(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("Pond createStorePosition parameter not match!");
			return false;
		} 
		context.getWriter().PREPARE(false, false);
		boolean state = false;
		if (context.getWriter().ISLINK()) {
			try { 
				String mainName = String.valueOf(args[0]); 
				String storeId = String.valueOf(args[1]);  
				state = context.getWriter().create(mainName, storeId, context.getInstanceConfig());    
			}catch (Exception e) {
				log.error("Create Store Position Exception",e);
			}finally {
				context.getWriter().REALEASE(false,state?false:true);
			}
		}
		return state;
	}

	/**
	 * @param args
	 *            parameter order is: String storeId,String keyColumn,String keyVal
	 */
	public static void deleteByKey(Context context, Object[] args) {
		boolean freeConn = false;
		if (!isValid(3, args)) {
			log.error("deleteByKey parameter not match!");
			return;
		}
		context.getWriter().PREPARE(false, false);
		if (context.getWriter().ISLINK()) {
			try {  
				String storeId = String.valueOf(args[0]);
				String keyColumn = String.valueOf(args[1]); 
				String keyVal = String.valueOf(args[2]);
				context.getWriter().delete(context.getInstanceConfig().getName(),storeId,keyColumn,keyVal);
			} catch (Exception e) {
				log.error("deleteByKey Exception", e);
				freeConn = true;
			} finally {
				context.getWriter().REALEASE(false,freeConn);
			}
		}
	}
	
	/**
	 * is a empty method
	 */
	public static void deleteByQuery(Context context, Object[] args) { 
		if (!isValid(1, args)) {
			log.error("deleteByQuery parameter not match!");
			return;
		}
		 
	}


	/**
	 * @param args
	 *            parameter order is: String mainName, String storeId
	 */
	public static void optimizeInstance(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("optimizeInstance parameter not match!");
			return;
		}
		context.getWriter().PREPARE(false, false);
		if (context.getWriter().ISLINK()) {
			try {
				String mainName = String.valueOf(args[0]); 
				String storeId = String.valueOf(args[1]);
				context.getWriter().optimize(mainName, storeId);
			} finally {
				context.getWriter().REALEASE(false,false);
			}
		}
	}
	
	/**
	 * @param args 
	 *            parameter order is: String instance, String seq, String storeId
	 */
	public static boolean switchInstance(Context context, Object[] args) {
		if (!isValid(3, args)) {
			log.error("switchInstance parameter not match!");
			return false;
		}
		String removeId = ""; 
		String mainName,storeId; 
		mainName = Common.getInstanceId(String.valueOf(args[0]),String.valueOf(args[1]));
		storeId = String.valueOf(args[2]); 
		int waittime=0; 
		if(GlobalParam.TASK_COORDER.checkFlowStatus(mainName,"",GlobalParam.JOB_TYPE.INCREMENT,STATUS.Running)) {
			GlobalParam.TASK_COORDER.setFlowStatus(mainName,"",GlobalParam.JOB_TYPE.INCREMENT.name(), STATUS.Blank, STATUS.Termination,
					context.getInstanceConfig().getPipeParams().showInfoLog());
			while (!GlobalParam.TASK_COORDER.checkFlowStatus(mainName,"",GlobalParam.JOB_TYPE.INCREMENT,STATUS.Ready)) {
				try {
					waittime++;
					Thread.sleep(2000);
					if (waittime > 30) {
						break;
					}
				} catch (InterruptedException e) {
					log.error("currentThreadState InterruptedException", e);
				}
			}  
		} 
		GlobalParam.TASK_COORDER.setFlowStatus(mainName,"",GlobalParam.JOB_TYPE.INCREMENT.name(), STATUS.Blank, STATUS.Termination,
				context.getInstanceConfig().getPipeParams().showInfoLog()); 
		context.getWriter().PREPARE(false, false);  
		if (context.getWriter().ISLINK()) {
			try {
				if(context.getInstanceConfig().getPipeParams().getWriteMechanism()==MECHANISM.AB) {
					if (storeId.equals("a")) {
						context.getWriter().optimize(mainName, "a");
						removeId = "b";
					} else {
						context.getWriter().optimize(mainName, "b");
						removeId = "a";
					}
					context.getWriter().removeInstance(mainName, removeId);
				} 
				context.getWriter().setAlias(mainName, storeId, context.getInstanceConfig().getAlias());
				return true;
			} catch (Exception e) {
				log.error("switchInstance Exception", e);
			} finally { 
				GlobalParam.TASK_COORDER.saveTaskInfo(String.valueOf(args[0]), String.valueOf(args[1]), storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
				GlobalParam.TASK_COORDER.setFlowStatus(mainName,"",GlobalParam.JOB_TYPE.INCREMENT.name(),STATUS.Blank,STATUS.Ready,
						context.getInstanceConfig().getPipeParams().showInfoLog());
				context.getWriter().REALEASE(false,false); 
			}
		}
		return false;
	}
 
	/**
	 * @param args
	 *            parameter order is: String mainName, boolean isIncrement 
	 */
	public static String getNewStoreId(Context context, Object[] args) throws EFException{
		String storeId = null;
		if (!isValid(2, args)) {
			log.error("getNewStoreId parameter not match!");
			return null;
		}
		String mainName = String.valueOf(args[0]);
		boolean isIncrement = (boolean) args[1]; 
		context.getWriter().PREPARE(false, false);
		boolean release = false;
		if (context.getWriter().ISLINK()) {
			try {
				storeId = context.getWriter().getNewStoreId(mainName, isIncrement, context.getInstanceConfig());
			} catch (Exception e) {
				release = true;
				throw new EFException(e,ELEVEL.Termination,ETYPE.RESOURCE_ERROR);	
			}finally {
				context.getWriter().REALEASE(false,release);
			}
		}
		return storeId;
	}
}
