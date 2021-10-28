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
		WRITE_POS_NOT_FOUND,RESOURCE_FAILURE,UNKNOWN;
	}

	private ELEVEL e_level = ELEVEL.Ignore;

	private ETYPE e_type = ETYPE.UNKNOWN;

	public EFException(String msg) {
		super(msg);
	} 
	
	public EFException(Exception e) {
		super(e);
	}
	
	public EFException(Exception e, ELEVEL elevel) {
		super(e);
		e_level = elevel;
	}
	
	public EFException(String msg, ELEVEL elevel) {
		super(msg);
		e_level = elevel;
	}
	
	public EFException(Exception e, ELEVEL elevel, ETYPE etype) {
		super(e);
		e_level = elevel;
		e_type = etype;
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
