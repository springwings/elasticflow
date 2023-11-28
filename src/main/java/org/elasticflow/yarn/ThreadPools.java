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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.elasticflow.task.TaskThread;
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

	private LinkedBlockingQueue<TaskThread> waitTask;

	private ThreadPoolExecutor cachedThreadPool;

	public ThreadPools(int minthread) {
		waitTask = new LinkedBlockingQueue<>();
		this.init(minthread);
	}

	public int getPoolSize() {
		return cachedThreadPool.getPoolSize();
	}

	public int getActiveCount() {
		return cachedThreadPool.getActiveCount();
	}

	public void init(int minthread) {
		cachedThreadPool = new ThreadPoolExecutor(minthread, minthread + 10, 1L, TimeUnit.SECONDS,
				new ArrayBlockingQueue<Runnable>(minthread * 5));
	}

	public void pushTask(TaskThread task) throws EFException {
		try {
			waitTask.put(task);
		} catch (Exception e) {
			throw new EFException(e, ELEVEL.Dispose);
		}
	}

	public void cleanWaitJob(String id) {
		Iterator<TaskThread> iter = waitTask.iterator();
		TaskThread job;
		while (iter.hasNext()) {
			job = iter.next();
			if (job.getId().equals(id))
				waitTask.remove(job);
		}
	}

	public void execute(Runnable job) {
		cachedThreadPool.execute(job);
	}

	public void start() {
		new Thread(() -> {
			try {
				while (true) {
					TaskThread job = waitTask.take();
					for (int i = 0; i < job.needThreads(); i++)
						execute(job);
				}
			} catch (Exception e) {
				Common.LOG.error("Start ThreadPools Exception", e);
			}
		}).start();
	}
}
