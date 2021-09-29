package org.elasticflow.flow.unit.handler;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.util.EFException;

/**
 * Date to millisecond timestamp
 * 
 * @author chengwen
 * @version 2.0
 * @date 2021-06-30 14:02
 */
public class ToTimestamp implements UnitHandler{
	
	private static DateTimeFormatter DFT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public void handle(PipeDataUnit u,EFField field, Object obj, Map<String, EFField> transParams) throws EFException {
		LocalDateTime localDate = LocalDateTime.parse(String.valueOf(obj), DFT); 
		u.getData().put(field.getName(),localDate.toInstant(ZoneOffset.of("+8")).toEpochMilli());
	}
}
