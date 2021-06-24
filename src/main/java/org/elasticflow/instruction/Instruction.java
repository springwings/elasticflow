/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.instruction;

import org.elasticflow.node.CPU;

/** 
 * @author chengwen
 * @version 1.0 
 */
public abstract class Instruction {  
	
	private String ID = CPU.getUUID(); 
	
	public String getID() {
		return ID;
	} 

	/**
	 * 
	 * @param length need parameter nums
	 * @param args 
	 * @return
	 */
	protected static boolean isValid(int length,Object... args) {
		if(args.length!=length) { 
			return false;
		}
		return true;
	};
	
}
