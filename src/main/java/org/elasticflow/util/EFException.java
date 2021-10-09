package org.elasticflow.util;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-07 14:12
 */
public class EFException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Ignore continue running with warning 
	 * Dispose need to fix and continue
	 * Termination, will stop thread 
	 * Stop, will stop program
	 */
	public static enum ELEVEL {
		Ignore, Dispose, Termination, Stop;
	}

	public static enum ETYPE {
		WRITE_POS_NOT_FOUND;
	}

	private ELEVEL e_level;

	private ETYPE e_type;

	public EFException(String msg) {
		super(msg);
		e_level = ELEVEL.Ignore;
	} 
	
	public EFException(Exception e) {
		super(e);
		e_level = ELEVEL.Ignore;
	}
	
	public EFException(String msg, ELEVEL etype) {
		super(msg);
		e_level = etype;
	}

	public EFException(String msg, ELEVEL elevel, ETYPE etype) {
		super(msg);
		e_level = elevel;
		e_type = etype;
	}

	public ELEVEL getErrorLevel() {
		return e_level;
	}

	public ETYPE getErrorType() {
		return e_type;
	} 
}
