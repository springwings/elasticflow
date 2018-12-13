package org.elasticflow.searcher.handler;

/**
 * user defined read data process function
 * @author chengwen
 * @version 1.0 
 */
public interface Handler{
	public <T>T Handle(Object... args);
}
