package org.elasticflow.util;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-07 14:12
 */
public class FNException extends Exception{ 
	
	private static final long serialVersionUID = 1L; 
	
	/**
	 * Ignore continue running with warnning
	 * Dispose need to fix and continue
	 * Termination stop thread
	 * Stop stop program
	 */
	public static enum ELEVEL {  
		Ignore,Dispose,Termination,Stop;
	}
	
	private ELEVEL e_level;
	
	public FNException(String msg){
		super(msg); 
	} 
	
	public FNException(String msg,ELEVEL level){ 
		super(msg); 
		e_level = level;
	} 
	
	public ELEVEL getErrorLevel() {
		return e_level;
	}
	 
	
	public FNException(Exception e){
		super(e); 
	}
}
