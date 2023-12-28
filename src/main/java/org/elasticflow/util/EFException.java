package org.elasticflow.util;

import org.elasticflow.config.GlobalParam.ELEVEL;
import org.elasticflow.config.GlobalParam.ETYPE;
import org.elasticflow.yarn.Resource;

/**
 * System global error definition
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-07 14:12
 */
public class EFException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Ignore continue running with warning ;
	 * Dispose need to fix and continue;
	 * Termination, will stop thread ;
	 * Stop, will stop program;
	 * BreakOff, open breaker
	 */

	private ELEVEL e_level = ELEVEL.Ignore;

	private ETYPE e_type = ETYPE.UNKNOWN;
	
	private StringBuffer track_info = new StringBuffer();

	public EFException(String msg) {
		super(msg);
	}
	public EFException(Exception e) {
		super(e);
	}

	public EFException(Exception e, ELEVEL elevel) {
		super(e);
		e_level = elevel;
		Resource.incrementErrorStates(elevel);
	}

	public EFException(String msg, ELEVEL elevel) {
		super(msg);
		e_level = elevel;
	} 
	
	public EFException(String message, ELEVEL elevel, ETYPE etype) { 
		super(message); 
		e_level = elevel;
		e_type = etype;
	} 
	
	public EFException(Exception e,String message) { 
		this(e,message,ELEVEL.Ignore,ETYPE.RESOURCE_ERROR);
		Resource.incrementErrorStates(ELEVEL.Ignore);
	}
	
	public EFException(Exception e,String message, ELEVEL elevel) { 
		this(e,message,elevel,ETYPE.RESOURCE_ERROR);
	}
	
	public EFException(Exception e,String message, ELEVEL elevel, ETYPE etype) { 
		super(message);
		initCause(e);
		e_level = elevel;
		e_type = etype;
		Resource.incrementErrorStates(e_level);
	} 
 
	@Override
	public String getMessage() { 
		return this.getTrack()+System.getProperty("line.separator")+super.getMessage();
	}   

	public ELEVEL getErrorLevel() {
		return e_level;
	}

	public ETYPE getErrorType() {
		return e_type;
	} 
	
	public void track(String info) {
		track_info.append(info);
		track_info.append(" >> ");
	}
	
	private String getTrack() {
		if(track_info.length()>0)
			return "TRACK INFOS:" + track_info.toString()+" error level "+e_level.name();  
		return "";
	}
}
