package org.elasticflow.model;

import java.util.ArrayList;

import org.elasticflow.node.CPU;

/**
 * Instruction data structure
 * @author chengwen
 * @version 1.0
 * @date 2018-06-22 09:08
 */
public class InstructionTree {

	private Node root;
	
	private String codeID;
 
    public InstructionTree(String val,String codeID){
        this.root = new Node(val);
        this.codeID = codeID;
    } 
    
    public Node getRoot() {
    	return this.root;
    }
    
    public Node addNode(String val,Node parent){
    	Node tn = new Node(val);
        parent.leaf.add(tn);
        return tn;
    }
 
    public Object depthRun(Node nodes){
    	Object[] args = new Object[nodes.leaf.size()];
		for(int i=0;i<nodes.leaf.size();i++) {
			if(nodes.leaf.get(i).leaf.size()==0) {
    			args[i] = nodes.leaf.get(i).value;
    		}else {
    			args[i] = depthRun(nodes.leaf.get(i));
    		} 
		} 
		int pos = nodes.value.lastIndexOf("."); 
		return CPU.RUN(codeID, nodes.value.substring(0, pos), nodes.value.substring(pos+1),true, args); 
    }
  
    
   public class Node{
        String value;
        ArrayList<Node> leaf = new ArrayList<>();  
        public Node(String value){
            this.value = value;
        }
    } 
}