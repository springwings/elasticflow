/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.piper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.computer.handler.ComputerHandler;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.ReaderState;
import org.elasticflow.node.CPU;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.handler.ReaderHandler;
import org.elasticflow.task.TaskThread;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.util.instance.PipeUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.writer.handler.WriterHandler;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coorder.TaskStateCoorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PipePump is the energy of the flow pipes
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-23 14:36
 */
public final class PipePump extends Instruction implements Serializable{

	private static final long serialVersionUID = 3783841547316513634L;

	private final static Logger log = LoggerFactory.getLogger("PipePump");
	
	private TaskStateCoorder taskStateControl;

	public static PipePump getInstance(ReaderFlowSocket reader, ComputerFlowSocket computer,
			List<WriterFlowSocket> writer, InstanceConfig instanceConfig) {
		return new PipePump(reader, computer, writer, instanceConfig);
	}

	private PipePump(ReaderFlowSocket reader, ComputerFlowSocket computer, List<WriterFlowSocket> writer,
			InstanceConfig instanceConfig) {
		CPU.prepare(getID(), instanceConfig, writer, reader, computer);
		taskStateControl = new TaskStateCoorder();
		try {
			if (instanceConfig.getReadParams().getHandler() != null) {
				try {
					reader.setReaderHandler((ReaderHandler) Class.forName(instanceConfig.getReadParams().getHandler())
							.getDeclaredConstructor().newInstance());
				} catch (Exception e) {
					if (GlobalParam.PLUGIN_CLASS_LOADER != null) {
						reader.setReaderHandler(
								(ReaderHandler) Class
										.forName(instanceConfig.getReadParams().getHandler(), true,
												GlobalParam.PLUGIN_CLASS_LOADER)
										.getDeclaredConstructor().newInstance());
					} else {
						throw new EFException(e, ELEVEL.Termination);
					}
				}
			}
			reader.setInstanceConfig(instanceConfig);
			if (computer != null) {
				if (instanceConfig.getComputeParams().getHandler() != null) {
					try {
						computer.setComputerHandler(
								(ComputerHandler) Class.forName(instanceConfig.getComputeParams().getHandler())
										.getDeclaredConstructor().newInstance());
					} catch (Exception e) {
						if (GlobalParam.PLUGIN_CLASS_LOADER != null) {
							computer.setComputerHandler(
									(ComputerHandler) Class
											.forName(instanceConfig.getComputeParams().getHandler(), true,
													GlobalParam.PLUGIN_CLASS_LOADER)
											.getDeclaredConstructor().newInstance());
						} else {
							throw new EFException(e, ELEVEL.Termination);
						}
					}
				}
				computer.setInstanceConfig(instanceConfig);
			}

			if (instanceConfig.getWriterParams().getHandler() != null) {
				for (WriterFlowSocket wfs : writer) {
					try {
						wfs.setWriteHandler((WriterHandler) Class.forName(instanceConfig.getWriterParams().getHandler())
								.getDeclaredConstructor().newInstance());
					} catch (Exception e) {
						if (GlobalParam.PLUGIN_CLASS_LOADER != null) {
							wfs.setWriteHandler(
									(WriterHandler) Class
											.forName(instanceConfig.getWriterParams().getHandler(), true,
													GlobalParam.PLUGIN_CLASS_LOADER)
											.getDeclaredConstructor().newInstance());
						} else {
							throw new EFException(e, ELEVEL.Termination);
						}
					}
					wfs.setInstanceConfig(instanceConfig);
				}
			}
		} catch (Exception e) {
			log.error("PipePump init Exception,", e);
			Common.stopSystem();
		}
	}

	/**
	 * Job running entry
	 * 
	 * @param instance
	 * @param storeId
	 * @param L1seq
	 * @param isFull
	 * @param writeInSamePosition
	 * @throws EFException
	 */
	public void run(String instance, String storeId, String L1seq, boolean isFull, boolean writeInSamePosition)
			throws EFException {
		JOB_TYPE job_type;
		String instanceId = Common.getInstanceId(instance, L1seq);
		String writeInstanceName = writeInSamePosition ? getInstanceConfig().getPipeParams().getInstanceName()
				: instance;
		if (isFull) {
			job_type = JOB_TYPE.FULL;
		} else {
			job_type = JOB_TYPE.INCREMENT;
		}
		Task task = Task.getInstance(instanceId, instance, L1seq, job_type, getInstanceConfig(), null);

		List<String> L2seqs = getInstanceConfig().getReadParams().getL2Seq().size() > 0
				? getInstanceConfig().getReadParams().getL2Seq()
				: Arrays.asList("");
		GlobalParam.TASK_COORDER.setFlowInfo(instance, job_type.name(), instanceId + " L2seqs nums", String.valueOf(L2seqs.size()));
		processFlow(task, instance, storeId, L2seqs, writeInstanceName, writeInSamePosition);
		GlobalParam.TASK_COORDER.resetFlowInfo(instance, job_type.name());
		if (isFull) {
			if (writeInSamePosition) {
				String destination = getInstanceConfig().getPipeParams().getInstanceName();				
				synchronized (GlobalParam.TASK_COORDER.getFlowInfo(destination, GlobalParam.JOB_TYPE.MASTER.name())) {
					String remainJobs = GlobalParam.TASK_COORDER.getFlowInfo(destination, GlobalParam.JOB_TYPE.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_JOBS.name());
					remainJobs = remainJobs.replace(instance, "").trim();
					GlobalParam.TASK_COORDER.setFlowInfo(destination, GlobalParam.JOB_TYPE.MASTER.name(),GlobalParam.FLOWINFO.FULL_JOBS.name(), remainJobs);				
					if (remainJobs.length() == 0) {
						CPU.RUN(getID(), "Pond", "switchInstance", true, instance, L1seq, storeId);
					}
				}
			} else {
				CPU.RUN(getID(), "Pond", "switchInstance", true, instance, L1seq, storeId);
			}
		}
	}

	public InstanceConfig getInstanceConfig() {
		return CPU.getContext(getID()).getInstanceConfig();
	}

	public ReaderFlowSocket getReader() {
		return CPU.getContext(getID()).getReader();
	}

	public ComputerFlowSocket getComputer() {
		return CPU.getContext(getID()).getComputer();
	}

	public WriterFlowSocket getWriter(Long outTime) {
		return CPU.getContext(getID()).getWriter(outTime);
	}

	public WriterFlowSocket getWriter() {
		return CPU.getContext(getID()).getWriter();
	}

	/**
	 * process resource data Flow
	 * 
	 * @param task
	 * @param mainName
	 * @param storeId
	 * @param L2seqs        example,L1 to database level,L2 to table level
	 * @param writeTo
	 * @param masterControl
	 * @throws EFException
	 */
	private void processFlow(Task task, String instance, String storeId, List<String> L2seqs, String writeInstanceName,
			boolean writeInSamePosition) throws EFException {
		for (String L2seq : L2seqs) {
			try {
				task.setL2seq(L2seq);
				GlobalParam.TASK_COORDER.setFlowInfo(task.getInstance(), task.getJobType().name(),task.getId() + L2seq,
						"start count page...");
				ConcurrentLinkedDeque<String> pageList = this.getPageLists(task);
				if (pageList == null)
					throw new EFException("Reader page split exception!", ELEVEL.Termination);
				processListsPages(task, writeInstanceName, pageList, storeId);
			} catch (EFException e) {
				if (task.getJobType().equals(JOB_TYPE.FULL) && !writeInSamePosition) {
					for (int t = 0; t < 5; t++) {
						getWriter().PREPARE(false, false);
						if (getWriter().ISLINK()) {
							try {
								getWriter().removeInstance(instance, storeId);
							} finally {
								getWriter().REALEASE(false, false);
							}
							break;
						}
					}
				}
				if (e.getErrorType().equals(ETYPE.WRITE_POS_NOT_FOUND)) {
					throw e;
				} else {
					log.error("[" + task.getJobType().name() + " " + instance + L2seq + "_" + storeId + " ERROR]", e);
					Resource.mailSender.sendHtmlMailBySynchronizationMode(" [EFLOWS] " + GlobalParam.RUN_ENV,
							"Job " + instance + " " + task.getJobType().name() + " Has stopped!");
				}
				Common.processErrorLevel(e);
			}
		}
	}

	private void processListsPages(Task task, String writeInstanceName, ConcurrentLinkedDeque<String> pageList,
			String storeId) throws EFException {
		String instanceId = Common.getInstanceId(task.getInstance(), task.getL1seq());
		int pageNum = pageList.size();
		if (pageNum == 0) {
			if (task.getInstanceConfig().getPipeParams().getLogLevel() == 0)
				log.info(Common.formatLog("start", "Complete " + task.getJobType().name(), instanceId, storeId,
						task.getL2seq(), 0, "", GlobalParam.TASK_COORDER.getLSeqPos(task.getInstance(),task.getL1seq(), task.getL2seq()),
						0, " no data!"));
		} else {
			if (task.getInstanceConfig().getPipeParams().getLogLevel() < 2)
				log.info(Common.formatLog("start",
						(getInstanceConfig().getPipeParams().isMultiThread() ? "MultiThread" : "SingleThread")
								+ " Start " + task.getJobType().name(),
						instanceId, storeId, task.getL1seq(), 0, "",
						GlobalParam.TASK_COORDER.getLSeqPos(task.getInstance(),task.getL1seq(), task.getL2seq()),
						0, ",totalpage:" + pageNum));

			long start = Common.getNow();
			AtomicInteger total = new AtomicInteger(0);
			if (getInstanceConfig().getPipeParams().isMultiThread()) {
				CountDownLatch taskSingal = new CountDownLatch(PipeUtil.estimateThreads(pageNum));
				Resource.ThreadPools
						.submitTask(new PumpThread(taskSingal, task, storeId, pageList, writeInstanceName, total,getInstanceConfig()));
				try {
					taskSingal.await();
				} catch (Exception e) {
					throw Common.getException(e);
				}
				if (task.taskState.getEfException() != null)
					throw task.taskState.getEfException();
			} else {
				currentThreadRun(task, storeId, pageList, writeInstanceName, total);
			}
			if (task.getInstanceConfig().getPipeParams().getLogLevel() < 2)
				log.info(
						Common.formatLog("complete", "Complete " + task.getJobType().name(), instanceId, storeId,
								task.getL2seq(), total.get(), "",
								GlobalParam.TASK_COORDER.getLSeqPos(task.getInstance(),task.getL1seq(), task.getL2seq()),
								Common.getNow() - start, ""));
			if (GlobalParam.TASK_COORDER.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination))
				throw new EFException(
						task.getInstance() + " " + task.getJobType().name() + " job has been Terminated!");
		}
	}

	/**
	 * use single thread process task, it is a safe mode support recover mechanism
	 * 
	 * @param pageList
	 * @throws EFException
	 */
	private void currentThreadRun(Task task, String storeId, ConcurrentLinkedDeque<String> pageList,
			String writeInstanceName, AtomicInteger total) throws EFException {
		ReaderState rState = null;
		int processPos = 0;
		String startId = "0";

		String scanField = task.getScanParam().getScanField();
		String keyField = task.getScanParam().getKeyField();
		String dataBoundary;
		int pageNum = pageList.size();

		boolean isUpdate = getInstanceConfig().getPipeParams().getWriteType().equals("increment") ? true : false;

		while (!pageList.isEmpty()) {
			dataBoundary = pageList.poll();
			processPos++;
			GlobalParam.TASK_COORDER.setFlowInfo(task.getInstance(), task.getJobType().name(),task.getId() + task.getL2seq(),
					processPos + "/" + pageList.size());			
			String dataScanDSL = PipeUtil.fillParam(task.getScanParam().getDataScanDSL(), PipeUtil.getScanParam(
					task.getL2seq(), startId, dataBoundary, task.getStartTime(), task.getEndTime(), scanField));
			if (GlobalParam.TASK_COORDER.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination)) {
				break;
			} else {
				DataPage pagedata = this.getPageData(
						Page.getInstance(keyField, scanField, startId, dataBoundary, getInstanceConfig(), dataScanDSL));
				if (getInstanceConfig().openCompute()) {
					pagedata = (DataPage) CPU.RUN(getID(), "ML", "compute", false, getID(), task.getJobType().name(),
							writeInstanceName, pagedata);
				}
				rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
						writeInstanceName, storeId, task.getL2seq(), pagedata, ",L1seq:"+task.getL1seq()+
						",process:" + processPos + "/" + pageNum,
						isUpdate, false);
				if (rState.isStatus() == false)
					throw new EFException("writeDataSet data exception!");
				total.getAndAdd(rState.getCount());
				startId = dataBoundary;
			}
			
			taskStateControl.setScanPosition(task, rState.getReaderScanStamp()); 
			 
			if (task.getJobType() == JOB_TYPE.INCREMENT) {
				GlobalParam.TASK_COORDER.saveTaskInfo(task.getInstance(), task.getL1seq(), storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
			}
		}
	}

	// thread safe get page list
	private ConcurrentLinkedDeque<String> getPageLists(Task task) {
		ConcurrentLinkedDeque<String> pageList = null;
		getReader().lock.lock();
		try {
			pageList = getReader().getPageSplit(task, getInstanceConfig().getPipeParams().getReadPageSize());
		} catch (Exception e) {
			log.error("get Page lists Exception]", e);
		} finally {
			getReader().lock.unlock();
		}
		return pageList;
	}

	// thread safe get page data
	private DataPage getPageData(Page pager) {
		getReader().lock.lock();
		DataPage pagedata = null;
		try {
			pagedata = (DataPage) CPU.RUN(getID(), "Pipe", "fetchPage", false, pager, getReader());
			getReader().freeJobPage();
		} catch (Exception e) {
			log.error("get Page Data Exception]", e);
		} finally {
			getReader().lock.unlock();
		}
		return pagedata;
	}
	
	/**
	 * use thread pool run task,it is not a steady mode if task fail will need re-do
	 * from start position.
	 * 
	 * @author chengwen
	 * @version 1.0
	 * @date 2019-01-11 10:45
	 * @modify 2019-01-11 10:45
	 */
	class PumpThread implements TaskThread {
		long start = Common.getNow();
		final int pageSize;
		final String ID = CPU.getUUID();
		final String writeInstanceName;
		final String storeId;
		final AtomicInteger total;

		Task task;
		CountDownLatch taskSingal;
		ReaderState rState = null;
		AtomicInteger processPos = new AtomicInteger(0);
		String startId = "0";
		ConcurrentLinkedDeque<String> pageList;
		InstanceConfig instanceConfig;
		boolean isUpdate = false;

		public PumpThread(CountDownLatch taskSingal, Task task, String storeId, ConcurrentLinkedDeque<String> pageList,
				String writeInstanceName, AtomicInteger total,InstanceConfig instanceConfig) {
			this.pageList = pageList;
			this.writeInstanceName = writeInstanceName;
			this.storeId = storeId;
			this.taskSingal = taskSingal;
			this.pageSize = pageList.size();
			this.total = total;
			this.task = task;
			this.instanceConfig = instanceConfig;
			if(instanceConfig.getPipeParams().getWriteType().equals("increment"))
				isUpdate = true;
		}
		
		@Override
		public String getId() {
			return ID;
		}
		
		@Override
		public int needThreads() {
			return PipeUtil.estimateThreads(this.pageSize);
		}

		@Override
		public void run() {
			String dataBoundary;
			while (!pageList.isEmpty()) {
				dataBoundary = pageList.poll();
				processPos.incrementAndGet();
				GlobalParam.TASK_COORDER.setFlowInfo(task.getInstance(), task.getJobType().name(),task.getId() + task.getL2seq(),
						processPos + "/" + this.pageSize);	
				String dataScanDSL = PipeUtil.fillParam(task.getScanParam().getDataScanDSL(),
						PipeUtil.getScanParam(task.getL2seq(), startId, dataBoundary, task.getStartTime(),
								task.getEndTime(), task.getScanParam().getScanField()));
				if (GlobalParam.TASK_COORDER.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(),
						STATUS.Termination)) {
					Resource.ThreadPools.cleanWaitJob(getId());
					Common.LOG.warn(task.getInstance() + " " + task.getJobType().name() + " job has been Terminated!");
					break;
				} else {
					DataPage pagedata = getPageData(
							Page.getInstance(task.getScanParam().getKeyField(), task.getScanParam().getScanField(),
									startId, dataBoundary, getInstanceConfig(), dataScanDSL));
					try {
						if (getInstanceConfig().openCompute()) {
							pagedata = (DataPage) CPU.RUN(getID(), "ML", "compute", false, getID(),
									task.getJobType().name(), writeInstanceName, pagedata);
						}
						rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
								writeInstanceName, storeId, task.getL2seq(), pagedata,
								",L1seq:"+task.getL1seq()+",process:" + processPos + "/" + pageSize, isUpdate, false);
					} catch (EFException e) {
						log.error("PumpThread", e);
						task.taskState.setEfException(e);
					} finally {
						if (rState == null || rState.isStatus() == false) {
							Common.LOG.warn("read data exception!");
							return;
						}						
						GlobalParam.TASK_COORDER.setFlowStatus(task.getInstance(), task.getL1seq(), GlobalParam.JOB_TYPE.FULL.name(),
								STATUS.Blank, STATUS.Ready, getInstanceConfig().getPipeParams().showInfoLog());
					}
					total.addAndGet(rState.getCount());
					startId = dataBoundary;
				}
				taskStateControl.setScanPosition(task, rState.getReaderScanStamp()); 
				if (task.getJobType() == JOB_TYPE.INCREMENT) {
					GlobalParam.TASK_COORDER.saveTaskInfo(task.getInstance(), task.getL1seq(), storeId,
							GlobalParam.JOB_INCREMENTINFO_PATH);
				}
			}
			taskSingal.countDown();
		}

	} 
}
