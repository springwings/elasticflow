package org.elasticflow.flow.unit.handler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;

/**
 * Date to millisecond timestamp
 * 
 * @author chengwen
 * @version 2.0
 * @date 2021-06-30 14:02
 */
public class ToTimestamp implements UnitHandler{
	
	private static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public void handle(PipeDataUnit u,EFField field, Object obj, Map<String, EFField> transParams) throws EFException {
		Date date;
		try {
			date = SDF.parse(String.valueOf(obj));
			u.getData().put(field.getName(),date.getTime());
		} catch (ParseException e) { 
			Common.LOG.error(field.getName()+" with reader key "+u.getReaderKeyVal()+",ToTimestamp parse exception!");
			throw new EFException(e.getMessage(), ELEVEL.Dispose);
		}        
	}

}
