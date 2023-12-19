/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.piper;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.computer.handler.ComputerHandler;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.TASK_STATUS;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.ReaderState;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.node.CPU;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.handler.ReaderHandler;
import org.elasticflow.task.TaskThread;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.util.instance.PipeUtil;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.writer.handler.WriterHandler;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PipePump is the energy of the flow pipes
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-23 14:36
 */
public final class PipePump extends Instruction implements Serializable {

	private static final long serialVersionUID = 3783841547316513634L;

	private final static Logger log = LoggerFactory.getLogger("PipePump");

	private TaskModel fullTask;

	private TaskModel incrementTask;

	private String instanceID;

	public static PipePump getInstance(String contextID, String instance, ReaderFlowSocket reader,
			ComputerFlowSocket computer, List<WriterFlowSocket> writer, InstanceConfig instanceConfig, String L1seq) {
		return new PipePump(contextID, instance, reader, computer, writer, instanceConfig, L1seq);
	}

	public String getInstanceID() {
		return this.instanceID;
	}

	private PipePump(String contextID, String instanceID, ReaderFlowSocket reader, ComputerFlowSocket computer,
			List<WriterFlowSocket> writer, InstanceConfig instanceConfig, String L1seq) {
		setID(contextID);
		CPU.prepare(contextID, instanceConfig, writer, reader, computer);
		this.instanceID = instanceID;
		fullTask = TaskModel.getInstance(instanceID, L1seq, JOB_TYPE.FULL, instanceConfig, null);
		incrementTask = TaskModel.getInstance(instanceID, L1seq, JOB_TYPE.INCREMENT, instanceConfig, null);
		try {
			if (instanceConfig.getReaderParams().getHandler() != null) {
				try {
					reader.setReaderHandler((ReaderHandler) Class.forName(instanceConfig.getReaderParams().getHandler())
							.getDeclaredConstructor().newInstance());
				} catch (Exception e) {
					if (GlobalParam.PLUGIN_CLASS_LOADER != null) {
						reader.setReaderHandler(
								(ReaderHandler) Class
										.forName(instanceConfig.getReaderParams().getHandler(), true,
												GlobalParam.PLUGIN_CLASS_LOADER)
										.getDeclaredConstructor().newInstance());
					} else {
						throw new EFException(e, ELEVEL.Termination);
					}
				}
			}
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
			}

			for (WriterFlowSocket wfs : writer) {
				if (instanceConfig.getWriterParams().getHandler() != null) {
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
				}
			}

		} catch (Exception e) {
			log.error("instance {} PipePump init exception",instanceID, e);
			Common.stopSystem(false);
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
	public void run(String storeId, String L1seq, boolean isFull, boolean isReferenceInstance) throws EFException {
		JOB_TYPE job_type;
		String instanceProcessId = TaskUtil.getInstanceProcessId(instanceID, L1seq);
		String destination = isReferenceInstance ? getInstanceConfig().getPipeParams().getReferenceInstance()
				: instanceID;
		TaskModel task;
		if (isFull) {
			job_type = JOB_TYPE.FULL;
			task = fullTask;
		} else {
			job_type = JOB_TYPE.INCREMENT;
			task = incrementTask;
		}
		List<String> L2seqs = getInstanceConfig().getReaderParams().getL2Seq();
		GlobalParam.TASK_COORDER.setFlowInfo(instanceID, job_type.name(), instanceProcessId + "_L2seqs_nums",
				String.valueOf(L2seqs.size()));
		processFlow(task, storeId, L2seqs, destination, isReferenceInstance);
		GlobalParam.TASK_COORDER.resetFlowInfo(instanceID, job_type.name());
		if (isFull) {
			if (isReferenceInstance) {
				synchronized (GlobalParam.TASK_COORDER.getFlowInfo(destination, GlobalParam.JOB_TYPE.VIRTUAL.name())) {
					String remainJobs = GlobalParam.TASK_COORDER
							.getFlowInfo(destination, GlobalParam.JOB_TYPE.VIRTUAL.name())
							.get(GlobalParam.FLOWINFO.FULL_JOBS.name());
					remainJobs = remainJobs.replace(instanceID, "").trim();
					GlobalParam.TASK_COORDER.setFlowInfo(destination, GlobalParam.JOB_TYPE.VIRTUAL.name(),
							GlobalParam.FLOWINFO.FULL_JOBS.name(), remainJobs);
					if (remainJobs.length() == 0)
						CPU.RUN(getID(), "Pond", "switchInstance", true, instanceID, L1seq, storeId);

				}
			} else {
				CPU.RUN(getID(), "Pond", "switchInstance", true, instanceID, L1seq, storeId);
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
	 * @param task
	 * @param storeId
	 * @param L2seqs       example,L1 to database level,L2 to table level
	 * @param destination
	 * @param isReferenceInstance
	 * @throws EFException
	 */
	private void processFlow(TaskModel task, String storeId, List<String> L2seqs, String destination,
			boolean isReferenceInstance) throws EFException {
		for (String L2seq : L2seqs) {
			try {
				task.setL2seq(L2seq);
				GlobalParam.TASK_COORDER.setFlowInfo(task.getInstanceID(), task.getJobType().name(),
						task.getInstanceProcessId() + L2seq, "start count page...");
				ConcurrentLinkedDeque<String> pageList = this.getPageLists(task);
				if (pageList == null) {
					log.warn("{} get data page list is null.",task.getInstanceProcessId());
					continue;
				}
				processListsPages(task, destination, pageList, storeId);
			} catch (EFException e) {
				if (task.getJobType().equals(JOB_TYPE.FULL) && !isReferenceInstance) {
					//For similar rollback scenarios
					for (int t = 0; t < 5; t++) {
						getWriter().PREPARE(false, false,false);
						if (getWriter().ISLINK()) {
							try {
								getWriter().removeInstance(instanceID, storeId);
							} finally {
								getWriter().REALEASE(false, false);
							}
							break;
						}
					}
				}
				e.track("[" + task.getJobType().name() + " " + instanceID + L2seq + "_" + storeId + " ERROR]");
				throw e;
			}
		}
	}

	private void processListsPages(TaskModel task, String destination, ConcurrentLinkedDeque<String> pageList,
			String storeId) throws EFException {
		String instanceProcessId = TaskUtil.getInstanceProcessId(task.getInstanceID(), task.getL1seq());
		int pageNum = pageList.size();
		if (pageNum == 0) {
			if (task.getInstanceConfig().getPipeParams().getLogLevel() == 0)
				log.info(TaskUtil.formatLog("start", task.getJobType().name(), instanceProcessId, storeId, task.getL2seq(), 0,
						"", GlobalParam.TASK_COORDER.getScanPositon(task.getInstanceID(), task.getL1seq(), task.getL2seq(),task.isfull()),
						0, " no data!"));
		} else {
			if (task.getInstanceConfig().getPipeParams().getLogLevel() < 2)
				log.info(TaskUtil.formatLog("start",
						(getInstanceConfig().getPipeParams().isMultiThread() ? "MultiThread" : "SingleThread") + " "
								+ task.getJobType().name(),
								instanceProcessId, storeId, task.getL2seq(), 0, "",
						GlobalParam.TASK_COORDER.getScanPositon(task.getInstanceID(), task.getL1seq(), task.getL2seq(),task.isfull()), 0,
						",totalpage:" + pageNum));

			long start = Common.getNow();
			AtomicInteger total = new AtomicInteger(0);
			if (getInstanceConfig().getPipeParams().isMultiThread()) {
				CountDownLatch taskSingal = new CountDownLatch(PipeUtil.estimateThreads(pageNum));
				Resource.threadPools.pushTask(
						new PumpThread(taskSingal, task, storeId, pageList, destination, total, getInstanceConfig()));
				try {
					taskSingal.await(90,TimeUnit.SECONDS);
				} catch (Exception e) {
					throw Common.convertException(e);
				}
				if (task.taskState.getEfException() != null)
					throw task.taskState.getEfException();
			} else {
				currentThreadRun(task, storeId, pageList, destination, total);
			}
			if (task.getInstanceConfig().getPipeParams().getLogLevel() < 2)
				log.info(TaskUtil.formatLog("complete", task.getJobType().name(), instanceProcessId, storeId, task.getL2seq(),
						total.get(), "",
						GlobalParam.TASK_COORDER.getScanPositon(task.getInstanceID(), task.getL1seq(), task.getL2seq(),task.isfull()),
						Common.getNow() - start, ""));
			this.breakCheck(task);
		}
	}

	private void breakCheck(TaskModel task) throws EFException {
		if (GlobalParam.TASK_COORDER.checkFlowStatus(task.getInstanceID(), task.getL1seq(), task.getJobType(),
				TASK_STATUS.Termination)) {
			throw new EFException(task.getInstanceID() + " " + task.getJobType().name() + " job has been Terminated!",
					ELEVEL.Dispose, ETYPE.EXTINTERRUPT);
		}
	}

	/**
	 * single thread process, it is a safe mode and support recover mechanism
	 * 
	 * @param task
	 * @param storeId     destination store id ,is time or a/b
	 * @param pageList
	 * @param destination store instance name, will concatenate store id
	 * @param total       process total data nums
	 * @throws EFException
	 */
	private void currentThreadRun(TaskModel task, String storeId, ConcurrentLinkedDeque<String> pageList, String destination,
			AtomicInteger total) throws EFException {
		ReaderState rState = null;
		int progressPos = 0;
		String startId = "0";

		String scanField = task.getScanParam().getScanField();
		String keyField = task.getScanParam().getKeyField();
		String dataBoundary;
		int pageNum = pageList.size();
		boolean isupdate = getInstanceConfig().getPipeParams().isUpdateWriteType();

		while (!pageList.isEmpty()) {
			dataBoundary = pageList.poll();
			progressPos++;
			GlobalParam.TASK_COORDER.setFlowInfo(task.getInstanceID(), task.getJobType().name(),
					task.getInstanceProcessId() + task.getL2seq(), progressPos + "/" + pageList.size());
			String dataScanDSL = PipeUtil.fillParam(task.getScanParam().getDataScanDSL(), PipeUtil.getScanParam(
					task.getL2seq(), startId, dataBoundary, task.getStartTime(), task.getEndTime(), scanField));

			this.breakCheck(task);

			DataPage pagedata = this.getPageData(
					TaskCursor.getInstance(keyField, scanField, startId, dataBoundary, getInstanceConfig(), dataScanDSL));
			if (getInstanceConfig().openCompute()) {
				long start = Common.getNow();
				int dataSize = pagedata.getData().size();
				String datab = pagedata.getDataBoundary();
				String scanStamp = pagedata.getScanStamp();
				pagedata = (DataPage) CPU.RUN(getID(), "ML", "compute", false, getID(), task.getJobType().name(),
						destination, pagedata);
				log.info(TaskUtil.formatLog("onepage", task.getJobType().name() + " Compute", task.getInstanceProcessId(), storeId,
						task.getL2seq(), dataSize, datab, scanStamp, Common.getNow() - start,
						",progress:" + progressPos + "/" + pageNum));
			}

			this.breakCheck(task);

			rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
					destination, storeId, task, pagedata,
					",progress:" + progressPos + "/" + pageNum, isupdate, false);
			if (rState.isStatus() == false)
				throw new EFException("writeDataSet data exception!");
			total.getAndAdd(rState.getCount());
			startId = dataBoundary;
 
			GlobalParam.TASK_COORDER.setScanPosition(task.getInstanceID(), task.getL1seq(), task.getL2seq(),
					rState.getReaderScanStamp(),false,task.isfull());
			GlobalParam.TASK_COORDER.saveTaskInfo(task.getInstanceID(), task.getL1seq(), storeId,task.isfull());
		}
	}

	// thread safe get page list
	private ConcurrentLinkedDeque<String> getPageLists(TaskModel task) {
		ConcurrentLinkedDeque<String> pageList = null;
		getReader().lock.lock();
		try {
			pageList = getReader().getDataPages(task, getInstanceConfig().getPipeParams().getReadPageSize());
		} catch (Exception e) {
			log.error("instance {} get page lists exception",task.getInstanceID(), e);
		} finally {
			getReader().lock.unlock();
		}
		return pageList;
	}

	// thread safe get page data
	private DataPage getPageData(TaskCursor pager) throws EFException {
		getReader().lock.lock();
		DataPage pagedata = null;
		try {
			pagedata = (DataPage) CPU.RUN(getID(), "Pipe", "fetchPage", false, pager, getReader());
			getReader().freeJobPage();
		} catch (Exception e) {
			throw new EFException(e,"pipepump get page data exception", ELEVEL.Dispose, ETYPE.DATA_ERROR);
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
		final int pageNum;
		final String ID = CPU.getUUID();
		final String destination;
		final String storeId;
		final AtomicInteger total;
		boolean isUpdate = false;

		TaskModel task;
		CountDownLatch taskSingal;
		ReaderState rState = null;
		AtomicInteger progressPos = new AtomicInteger(0);
		String startId = "0";
		ConcurrentLinkedDeque<String> pageList;
		InstanceConfig instanceConfig;

		public PumpThread(CountDownLatch taskSingal, TaskModel task, String storeId, ConcurrentLinkedDeque<String> pageList,
				String destination, AtomicInteger total, InstanceConfig instanceConfig) {
			this.pageList = pageList;
			this.destination = destination;
			this.storeId = storeId;
			this.taskSingal = taskSingal;
			this.pageNum = pageList.size();
			this.total = total;
			this.task = task;
			this.instanceConfig = instanceConfig;
			this.isUpdate = instanceConfig.getPipeParams().isUpdateWriteType();
		}

		@Override
		public String getId() {
			return ID;
		}

		@Override
		public int needThreads() {
			return PipeUtil.estimateThreads(this.pageNum);
		}

		@Override
		public void run() {
			String dataBoundary;
			while (!pageList.isEmpty()) {
				dataBoundary = pageList.poll();
				progressPos.incrementAndGet();
				GlobalParam.TASK_COORDER.setFlowInfo(task.getInstanceID(), task.getJobType().name(),
						task.getInstanceProcessId() + task.getL2seq(), progressPos + "/" + this.pageNum);
				String dataScanDSL = PipeUtil.fillParam(task.getScanParam().getDataScanDSL(),
						PipeUtil.getScanParam(task.getL2seq(), startId, dataBoundary, task.getStartTime(),
								task.getEndTime(), task.getScanParam().getScanField()));
				if (GlobalParam.TASK_COORDER.checkFlowStatus(task.getInstanceID(), task.getL1seq(), task.getJobType(),
						TASK_STATUS.Termination)) {
					Resource.threadPools.cleanWaitJob(getId());
					Common.LOG
							.warn(task.getInstanceID() + " " + task.getJobType().name() + " job has been Terminated!");
					break;
				}

				try {
					DataPage pagedata = getPageData(
							TaskCursor.getInstance(task.getScanParam().getKeyField(), task.getScanParam().getScanField(),
									startId, dataBoundary, getInstanceConfig(), dataScanDSL));
					if (getInstanceConfig().openCompute()) {
						long start = Common.getNow();
						int dataSize = pagedata.getData().size();
						String datab = pagedata.getDataBoundary();
						String scanStamp = pagedata.getScanStamp();
						pagedata = (DataPage) CPU.RUN(getID(), "ML", "compute", false, getID(),
								task.getJobType().name(), destination, pagedata);
						log.info(TaskUtil.formatLog("onepage", task.getJobType().name() + " Compute", task.getInstanceProcessId(),
								storeId, task.getL2seq(), dataSize, datab, scanStamp, Common.getNow() - start,
								",progress:" + progressPos + "/" + pageNum));
					}
					if (GlobalParam.TASK_COORDER.checkFlowStatus(task.getInstanceID(), task.getL1seq(),
							task.getJobType(), TASK_STATUS.Termination)) {
						Resource.threadPools.cleanWaitJob(getId());
						Common.LOG.warn("{} {} job has been Terminated!",task.getInstanceID(),task.getJobType().name());
						break;
					}
					rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
							task.getInstanceProcessId(), storeId, task, pagedata,
							",progress:" + progressPos + "/" + pageNum, this.isUpdate,
							false);
				} catch (EFException e) {
					log.error("instance {} process page data exception", this.instanceConfig.getInstanceID(),e);
					task.taskState.setEfException(e);
				} finally {
					if (rState == null || rState.isStatus() == false) {
						Common.LOG.warn("instance {} read data exception!",instanceConfig.getInstanceID());
						return;
					}
					GlobalParam.TASK_COORDER.setFlowStatus(task.getInstanceID(), task.getL1seq(),
							GlobalParam.JOB_TYPE.FULL.name(), TASK_STATUS.Blank, TASK_STATUS.Ready,
							getInstanceConfig().getPipeParams().showInfoLog());
				}
				total.addAndGet(rState.getCount());
				startId = dataBoundary; 
				GlobalParam.TASK_COORDER.setScanPosition(task.getInstanceID(), task.getL1seq(), task.getL2seq(),
						rState.getReaderScanStamp(),false,task.isfull());
				GlobalParam.TASK_COORDER.saveTaskInfo(task.getInstanceID(), task.getL1seq(), storeId,task.isfull());
			}
			taskSingal.countDown();
		}

	}
}
