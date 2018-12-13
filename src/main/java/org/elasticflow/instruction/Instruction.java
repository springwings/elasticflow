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
