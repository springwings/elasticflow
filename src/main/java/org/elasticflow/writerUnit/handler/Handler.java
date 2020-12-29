package org.elasticflow.writerUnit.handler;

import java.util.Map;

import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;

/**
 * user defined data unit process function
 * store common function handler
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:11
 */
public interface Handler { 
	void handle(PipeDataUnit u,Object obj,Map<String, EFField> transParams); 
}
