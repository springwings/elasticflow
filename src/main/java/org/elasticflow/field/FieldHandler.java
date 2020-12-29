package org.elasticflow.field;

import org.elasticflow.util.EFException;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-20 10:47
 */
public interface FieldHandler{ 
	
	public void parse(String s) throws EFException;
	
}
