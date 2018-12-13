package org.elasticflow.task;

import java.util.ArrayList;

import org.elasticflow.model.InstructionTree;
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

	public void runInstructions() {
		ArrayList<InstructionTree> Instructions = Resource.nodeConfig.getInstructions().get(this.codeID).getCode(); 
		for(InstructionTree Instruction:Instructions ) {
			Instruction.depthRun(Instruction.getRoot());
		}
	}
}
