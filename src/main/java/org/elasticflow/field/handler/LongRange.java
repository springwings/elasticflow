package org.elasticflow.field.handler;

import org.elasticflow.field.FieldHandler;
import org.elasticflow.util.EFException;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-20 10:47
 */
public class LongRange implements FieldHandler{
	
	protected Long min;
	protected Long max; 
	public final static String RangeSeperator = "_"; 
 
	public static LongRange valueOf(String s)
			throws EFException {
		LongRange ir = new LongRange();
		ir.parse(s);
		return ir;
	}
	
	public LongRange() {
		min = 0l;
		max = Long.MAX_VALUE;
	} 
	
	@Override
	public void parse(String s) throws EFException {  
		if (s == null) {
			throw new EFException("parse error with value is null!");
		} 
		int seg = s.indexOf(RangeSeperator);
		if (seg >= s.length()) {
			throw new NumberFormatException(s);
		}  
		if(seg<0){ 
			this.setMax(Long.valueOf(s));
			this.setMin(Long.valueOf(s));
		}else {
			try {
				if (seg > 0){
					String minStr = s.substring(0, seg); 
					Long min =  Long.valueOf(minStr);
					this.setMin(min);
				}
				if (seg < s.length()-1){
					String maxStr = s.substring(seg+1);
					Long max = Long.valueOf(maxStr);
					this.setMax(max);
				}
			} catch (Exception e) {
				throw new EFException(e);
			}
		} 
	} 
	public boolean isValid() {
		return max.compareTo(min) >= 0;
	}
	public Object getMin() {
		return min;
	}
	public void setMin(Long min) {
		this.min = min;
	}
	public Object getMax() {
		return max;
	}
	public void setMax(Long max) {
		this.max = max;
	}
}
