/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;

/**
 * Running thread resources center
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:54 
 */
public class ThreadPools {

	private ArrayBlockingQueue<PipePump.PumpThread> waitJob;

	private ThreadPoolExecutor cachedThreadPool;
	
	public ThreadPools(int instanceNum) {
		waitJob = new ArrayBlockingQueue<>(instanceNum * 5);
		cachedThreadPool = new ThreadPoolExecutor(instanceNum, instanceNum*3,
	            1L, TimeUnit.SECONDS,
	            new SynchronousQueue<Runnable>());
	}

	public void submitJobPage(PipePump.PumpThread jobPage) throws EFException {
		try {
			waitJob.put(jobPage);
		} catch (Exception e) {
			throw new EFException(e,ELEVEL.Dispose);
		}
	}
	
	public void cleanWaitJob(String id) {
		Iterator<PipePump.PumpThread> iter = waitJob.iterator();
		PipePump.PumpThread job;
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
					PipePump.PumpThread job = waitJob.take(); 
					for(int i=0;i<job.needThreads();i++)
						cachedThreadPool.execute(job);
				}  
			} catch (Exception e) {
				Common.LOG.error("Start ThreadPools Exception", e);
			}
		}).start();
	}  
}
