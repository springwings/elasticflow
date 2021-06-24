package org.elasticflow.util;

import java.io.Serializable;

/**
 * Optimizing java to return multiple values
 * 
 * @author chengwen
 * @version 1.0
 * @date 2020-10-31 13:55
 * @modify 2021-01-10 09:45
 */
public class EFTuple<T1 extends Serializable, T2 extends Serializable> implements Serializable {
	private static final long serialVersionUID = 1L;
	public final T1 v1;
	public final T2 v2;
	
	public EFTuple(T1 v1, T2 v2) {
		this.v1 = v1;
		this.v2 = v2;
	}
}
