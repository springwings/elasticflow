/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction.sets;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.config.GlobalParam.TASK_FLOW_SINGAL;
import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.TaskUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pond operation Instruction sets 
 * It is an instruction code,Can only interpret
 * Maintenance of Management Data Pools
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:25
 */
public class Pond extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Pond");

	/**
	 * Create service instance storage location
	 * @param context: context object
	 * @param args     : [String mainName,String storeId]
	 */
	public static boolean createStorePosition(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("instruction.set.Pond.createStorePosition parameter not match!");
			return false;
		}
		context.getWriter().PREPARE(false, false);
		boolean state = false;
		if (context.getWriter().connStatus()) {
			String instanceID = String.valueOf(args[0]);
			try {
				String storeId = String.valueOf(args[1]);
				state = context.getWriter().create(instanceID, storeId, context.getInstanceConfig());
			} catch (Exception e) {
				log.error("instruction.set.Pond.createStorePosition run instance {} exception",instanceID, e);
			} finally {
				context.getWriter().releaseConn(false, state ? false : true);
			}
		}
		return state;
	}

	/**
	 * Select and delete service instance content based on ID
	 * @param context: context object
	 * @param args     : [String storeId,String keyColumn,String keyVal]
	 */
	public static void deleteByKey(Context context, Object[] args) {
		boolean freeConn = false;
		if (!isValid(3, args)) {
			log.error("instruction.set.Pond.deleteByKey parameter not match!");
			return;
		}
		context.getWriter().PREPARE(false, false);
		if (context.getWriter().connStatus()) {
			String storeId = String.valueOf(args[0]);
			try {				
				String keyColumn = String.valueOf(args[1]);
				String keyVal = String.valueOf(args[2]);
				context.getWriter().delete(context.getInstanceConfig().getInstanceID(), storeId, keyColumn, keyVal);
			} catch (Exception e) {
				log.error("instruction.set.Pond.deleteByKey store id {} exception",storeId, e);
				freeConn = true;
			} finally {
				context.getWriter().releaseConn(false, freeConn);
			}
		}
	}

	/**
	 * Batch service instance content through query
	 * is a empty method
	 */
	public static void deleteByQuery(Context context, Object[] args) {
		if (!isValid(1, args)) {
			log.error("instruction.set.Pond.deleteByQuery parameter not match!");
			return;
		}

	}

	/**
	 * Optimize service instances
	 * @param context: context object
	 * @param args     : [String mainName, String storeId]
	 */
	public static void optimizeInstance(Context context, Object[] args) {
		if (!isValid(2, args)) {
			log.error("instruction.set.Pond.optimizeInstance parameter not match!");
			return;
		}
		context.getWriter().PREPARE(false, false);
		if (context.getWriter().connStatus()) {
			try {
				String mainName = String.valueOf(args[0]);
				String storeId = String.valueOf(args[1]);
				context.getWriter().optimize(mainName, storeId);
			} finally {
				context.getWriter().releaseConn(false, false);
			}
		}
	}

	/**
	 * Switching service instances
	 * @param context: context object
	 * @param args:    [String instance, String seq, String storeId]
	 */
	public static boolean switchInstance(Context context, Object[] args) {
		if (!isValid(3, args)) {
			log.error("instruction.set.Pond.switchInstance parameter not match!");
			return false;
		}
		String removeId = "";
		String instanceProcessId, storeId;
		instanceProcessId = TaskUtil.getInstanceProcessId(String.valueOf(args[0]), String.valueOf(args[1]));
		storeId = String.valueOf(args[2]);
		int waittime = 0;
		if (GlobalParam.TASK_COORDER.checkFlowSingal(instanceProcessId, "", GlobalParam.JOB_TYPE.INCREMENT, TASK_FLOW_SINGAL.Running)) {
			GlobalParam.TASK_COORDER.setFlowSingal(instanceProcessId, "", GlobalParam.JOB_TYPE.INCREMENT.name(), TASK_FLOW_SINGAL.Blank,
					TASK_FLOW_SINGAL.Termination, context.getInstanceConfig().getPipeParams().showInfoLog());
			while (!GlobalParam.TASK_COORDER.checkFlowSingal(instanceProcessId, "", GlobalParam.JOB_TYPE.INCREMENT,
					TASK_FLOW_SINGAL.Ready)) {
				try {
					waittime++;
					Thread.sleep(2000);
					if (waittime > 30) {
						break;
					}
				} catch (InterruptedException e) {
					log.error("current thread sleep exception", e);
				}
			}
		}
		GlobalParam.TASK_COORDER.setFlowSingal(instanceProcessId, "", GlobalParam.JOB_TYPE.INCREMENT.name(), TASK_FLOW_SINGAL.Blank,
				TASK_FLOW_SINGAL.Termination, context.getInstanceConfig().getPipeParams().showInfoLog());
		context.getWriter().PREPARE(false, false);
		if (context.getWriter().connStatus()) {
			try {
				if (context.getInstanceConfig().getPipeParams().getWriteMechanism() == MECHANISM.AB) {
					if (storeId.equals("a")) {
						context.getWriter().optimize(instanceProcessId, "a");
						removeId = "b";
					} else {
						context.getWriter().optimize(instanceProcessId, "b");
						removeId = "a";
					}
					context.getWriter().removeShard(instanceProcessId, removeId);
				}
				context.getWriter().setAlias(instanceProcessId, storeId, context.getInstanceConfig().getAlias());
				return true;
			} catch (Exception e) {
				log.error("instruction.set.Pond.switchInstance instance {} exception",instanceProcessId, e);
			} finally {
				GlobalParam.TASK_COORDER.saveTaskInfo(String.valueOf(args[0]), String.valueOf(args[1]), storeId, false);
				GlobalParam.TASK_COORDER.setFlowSingal(instanceProcessId, "", GlobalParam.JOB_TYPE.INCREMENT.name(),
						TASK_FLOW_SINGAL.Blank, TASK_FLOW_SINGAL.Ready, context.getInstanceConfig().getPipeParams().showInfoLog());
				context.getWriter().releaseConn(false, false);
			}
		}
		return false;
	}

	/**
	 * Obtain a new storage location identifier
	 * 
	 * @param context: context object
	 * @param args     :[String mainName, boolean isIncrement]
	 */
	public static String getNewStoreId(Context context, Object[] args) throws EFException {
		String storeId = null;
		if (!isValid(2, args)) {
			log.error("instruction.set.Pond.getNewStoreId parameter not match!");
		} else {
			String mainName = String.valueOf(args[0]);
			boolean isIncrement = (boolean) args[1];
			context.getWriter().PREPARE(false, false);
			boolean release = false;
			if (context.getWriter().connStatus()) {
				try {
					storeId = context.getWriter().getNewStoreId(mainName, isIncrement, context.getInstanceConfig());
				} catch (Exception e) {
					release = true;
					throw new EFException(e,
							"instance " + mainName + " failed to obtain storage location from "
									+ context.getInstanceConfig().getPipeParams().getWriteTo(),
							ELEVEL.Termination, ETYPE.RESOURCE_ERROR);
				} finally {
					context.getWriter().releaseConn(false, release);
				}
			}
		}
		return storeId;
	}
}
