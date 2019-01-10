package org.elasticflow.flow;

import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.connect.FnConnectionPool;
import org.elasticflow.connect.FnConnectionSocket;
import org.elasticflow.param.pipe.ConnectParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory; 
/**
 * data pipe flow model 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 10:52
 */
public abstract class Flow {
	
	protected volatile FnConnectionSocket<?> FC;
	
	protected String poolName; 
	
	protected ConnectParams connectParams;
	
	protected AtomicInteger retainer = new AtomicInteger(0);
	
	private final static Logger log = LoggerFactory.getLogger(Flow.class);
	
	public abstract void INIT(ConnectParams connectParams);
	
	public FnConnectionSocket<?> PREPARE(boolean isMonopoly,boolean canSharePipe) {  
		if(isMonopoly) {
			synchronized (this.FC) {
				if(this.FC==null) 
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe); 
			} 
		}else {
			synchronized (this.retainer) {  
				if(this.retainer.getAndIncrement()==0) {
					this.FC = FnConnectionPool.getConn(this.connectParams,
							this.poolName,canSharePipe);  
				} 
			} 
		}  
		return this.FC;
	}
	
	public void REALEASE(boolean isMonopoly,boolean releaseConn) { 
		if(isMonopoly==false) { 
			synchronized(retainer){ 
				if(releaseConn)
					retainer.set(0);
				if(retainer.decrementAndGet()<=0){
					FnConnectionPool.freeConn(this.FC, this.poolName,releaseConn);  
					this.FC = null;
					retainer.set(0); 
				}else{
					log.info(this.FC+" retainer is "+retainer.get());
				}
			} 
		} 
	}   
	
	public FnConnectionSocket<?> GETSOCKET() {
		return this.FC;
	}
	
	public boolean ISLINK() {
		if(this.FC==null) 
			return false;
		return true;
	}  
	
	public void freeConnPool() {
		FnConnectionPool.release(this.poolName);
	}
}
