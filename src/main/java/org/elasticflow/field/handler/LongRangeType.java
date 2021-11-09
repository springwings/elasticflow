package org.elasticflow.field.handler;

import org.elasticflow.field.FieldHandler;
import org.elasticflow.util.EFException;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-20 10:47
 */
public class LongRangeType implements FieldHandler<Long> {

	protected Long min;
	protected Long max;
	protected Long val;
	public final static String RangeSeperator = "_";
	
	public LongRangeType(Object data) throws EFException { 
		parse(data);
	}

	public Long getVal() {
		return this.val;
	}

	public LongRangeType() {
		min = 0l;
		max = Long.MAX_VALUE;
	}
	 
	public Long parse(Object s) throws EFException {
		if (s == null) {
			throw new EFException("parse error with value is null!");
		}
		String val = String.valueOf(s);
		int seg = val.indexOf(RangeSeperator);
		if (seg >= val.length()) {
			throw new NumberFormatException(val);
		}
		
		if (seg < 0) {
			this.val = Long.valueOf(val);
			this.max = this.val;
			this.min = this.val;
		} else {
			try {
				if (seg > 0) {
					String minStr = val.substring(0, seg);
					this.min = Long.valueOf(minStr);
				}
				if (seg < val.length() - 1) {
					String maxStr = val.substring(seg + 1);
					this.max = Long.valueOf(maxStr);
				}
				this.val = this.min;
			} catch (Exception e) {
				throw new EFException(e);
			}
		}
		return this.val;
	}
	
	@Override
	public String toString() {
		return String.valueOf(this.getVal());
	}
	
	@Override
	public boolean isValid() {
		return max.compareTo(min) >= 0;
	}

	public Object getMin() {
		return min;
	}

	public Object getMax() {
		return max;
	} 
}
