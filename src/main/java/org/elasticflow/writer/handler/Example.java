package org.elasticflow.writer.handler;

import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.writer.flow.SolrFlow;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:11
 */
public class Example extends SolrFlow{
 
	public static Example getInstance(ConnectParams connectParams) {
		Example o = new Example();
		o.INIT(connectParams);
		return o;
	}
}
