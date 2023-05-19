package org.elasticflow.field.handler;

import org.elasticflow.field.FieldHandler;
import org.elasticflow.util.EFException;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-11-20 10:47
 */
public class FloatRangeType implements FieldHandler<Double[]> {

	public final static String RangeSeperator = "_";
	
	public static Float valueOf(Object data) throws EFException { 
		return Float.valueOf(String.valueOf(data));
	}
	
	/**
	 * 
	 * @param data
	 * @return 0 min  1 max
	 * @throws EFException
	 */
	public static Float[] parse(Object data) throws EFException {  
		Float[] res = new Float[]{0F,Float.MAX_VALUE};
		if (data == null) {
			throw new EFException("parse error with value is null!");
		}
		String val = String.valueOf(data);
		int seg = val.indexOf(RangeSeperator);
		if (seg >= val.length()) {
			throw new NumberFormatException(val);
		}
		
		if (seg < 0) {
			res[0] = Float.valueOf(val);
			res[1] = res[0];
		} else {
			try {
				if (seg > 0) {
					String minStr = val.substring(0, seg);
					res[0] = Float.valueOf(minStr);
				}
				if (seg < val.length() - 1) {
					String maxStr = val.substring(seg + 1);
					res[1] = Float.valueOf(maxStr);
				}
			} catch (Exception e) {
				throw new EFException(e);
			}
		}
		return res;
	}
	
}
