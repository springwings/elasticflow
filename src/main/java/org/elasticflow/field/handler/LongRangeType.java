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

	public static LongRangeType valueOf(String s) throws EFException {
		LongRangeType ir = new LongRangeType();
		ir.parse(s);
		return ir;
	}

	@Override
	public Long getVal() {
		return this.val;
	}

	public LongRangeType() {
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
		
		if (seg < 0) {
			this.val = Long.valueOf(s);
			this.max = this.val;
			this.min = this.val;
		} else {
			try {
				if (seg > 0) {
					String minStr = s.substring(0, seg);
					this.min = Long.valueOf(minStr);
				}
				if (seg < s.length() - 1) {
					String maxStr = s.substring(seg + 1);
					this.max = Long.valueOf(maxStr);
				}
				this.val = this.min;
			} catch (Exception e) {
				throw new EFException(e);
			}
		}
	}
	
	public String valueOf(Object val) {
		return String.valueOf(val);
	}
	
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
