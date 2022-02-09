package org.elasticflow.model;

import org.elasticflow.util.EFException;

/**
 * Task Running State
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-08 16:49
 */
public class TaskState {
	
	private EFException efException;

	public EFException getEfException() {
		return efException;
	}

	public void setEfException(EFException efException) {
		this.efException = efException;
	}
	
	
}
