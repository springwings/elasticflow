package org.elasticflow.model;

import java.util.concurrent.ConcurrentHashMap;

import org.elasticflow.util.Common;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-22 09:08
 */
public class EFState<T> extends ConcurrentHashMap<String,T>{
 
	private static final long serialVersionUID = 7134367712318896122L;
	
	public void set(String instance,T dt) {
		put(instance, dt);
	} 
	
	public void set(String instance,String L1seq,T dt) {
		put(Common.getMainName(instance, L1seq), dt);
	} 
	
	public void set(String instance,String L1seq,String tag,T dt) {
		put(Common.getMainName(instance, L1seq)+tag, dt);
	}  
	
	public T get(String instance,String L1seq) {
		return get(Common.getMainName(instance, L1seq));
	}
	
	public T get(String instance,String L1seq,String tag) {
		return get(Common.getMainName(instance, L1seq)+tag);
	}
	
	public boolean containsKey(String instance,String L1seq) {
		return containsKey(Common.getMainName(instance, L1seq));
	}
	 
}
