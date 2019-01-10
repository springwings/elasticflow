package org.elasticflow.yarn;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;

/**
 * Running thread resources center
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:54 
 */
public class ThreadPools {

	private ArrayBlockingQueue<PipePump.Pump> waitJob = new ArrayBlockingQueue<>(GlobalParam.POOL_SIZE * 10);

	private int maxThreadNums = GlobalParam.POOL_SIZE;

	ThreadPoolExecutor cachedThreadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            30L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());

	public void submitJobPage(PipePump.Pump jobPage) {
		try {
			waitJob.put(jobPage);
		} catch (Exception e) {
			Common.LOG.error("SubmitJobPage Exception", e);
		}
	}
	
	public void cleanWaitJob(String id) {
		Iterator<PipePump.Pump> iter = waitJob.iterator();
		PipePump.Pump job;
        while(iter.hasNext()) {
        	job = iter.next();
        	if(job.getId().equals(id))
        		waitJob.remove(job);
        }
	}

	public void start() {
		new Thread(() -> {
			try {
				while(true) {
					PipePump.Pump job = waitJob.take();
					while(cachedThreadPool.getTaskCount()>=maxThreadNums) {
						Thread.sleep(900);
					} 
					for(int i=0;i<job.needThreads();i++)
						cachedThreadPool.execute(job);
				}  
			} catch (Exception e) {
				Common.LOG.error("Start ThreadPools Exception", e);
			}
		}).start();
	}  
}
