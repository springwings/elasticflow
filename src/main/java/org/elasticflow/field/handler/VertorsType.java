package org.elasticflow.field.handler;

import java.util.ArrayList;
import java.util.List;

import org.elasticflow.field.FieldHandler;
import org.elasticflow.util.EFException;

/**
* @description 
* @author chengwen
* @version 1.0
* @data 2021-01-12
*/

public class VertorsType implements FieldHandler<List<Float>>{
	
	protected List<Float> vals;
	
	public static VertorsType valueOf(String s)
			throws EFException {
		VertorsType vc = new VertorsType();
		vc.parse(s);
		return vc;
	}
	
	
	/**
	 * @param s, "0,1,1,2,0.5"
	 */
	@Override
	public void parse(String s) throws EFException {
		this.vals = new ArrayList<>();
		String[] tmp = s.split(",");
		for(String k:tmp) {
			this.vals.add(Float.parseFloat(k));
		}
	}


	@Override
	public List<Float> getVal() { 
		return this.vals;
	}

}
