/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.task.mode;

import java.util.ArrayList;

import org.elasticflow.model.InstructionTree;
import org.elasticflow.util.EFException;
import org.elasticflow.yarn.Resource;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:22
 */
public class InstructionTask {

	private String codeID;

	public static InstructionTask createTask(String id) {
		InstructionTask tk = new InstructionTask();
		tk.codeID = id;
		return tk;
	}

	public void runInstructions() throws EFException {
		ArrayList<InstructionTree> Instructions = Resource.nodeConfig.getInstructions().get(this.codeID).getCode(); 
		for(InstructionTree Instruction:Instructions ) {
			Instruction.depthRun(Instruction.getRoot());
		}
	}
}
