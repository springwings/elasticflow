/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.writerUnit.handler;

import java.util.Map;

import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.util.EFException;

/**
 * user defined data unit process function
 * store common function handler
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:11
 */
public interface Handler { 
	void handle(PipeDataUnit u,EFField field,Object obj,Map<String, EFField> transParams) throws EFException; 
}
