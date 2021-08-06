package org.elasticflow.writerUnit.handler;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.util.Common;

/**
 * Date to millisecond timestamp
 * 
 * @author chengwen
 * @version 2.0
 * @date 2021-06-30 14:02
 */
public class ToTimestamp implements WriterUnitHandler{
	
	private static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public void handle(PipeDataUnit u,EFField field, Object obj, Map<String, EFField> transParams) {
		Date date;
		try {
			date = SDF.parse(String.valueOf(obj));
			u.getData().put(field.getName(),date.getTime());
		} catch (ParseException e) {
			Common.LOG.error("ToTimestamp parse exception!",e);
		}        
	}

}
