/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.piper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.computer.handler.ComputeHandler;
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
import org.elasticflow.reader.handler.ReadHandler;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.util.PipeNormsUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.writer.handler.WriteHandler;
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
public final class PipePump extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("PipePump");
	


	public static PipePump getInstance(ReaderFlowSocket reader,
			ComputerFlowSocket computer,List<WriterFlowSocket> writer, InstanceConfig instanceConfig) {
		return new PipePump(reader, computer,writer, instanceConfig);
	}

	private PipePump(ReaderFlowSocket reader, ComputerFlowSocket computer,
			List<WriterFlowSocket> writer,InstanceConfig instanceConfig) {
		CPU.prepare(getID(), instanceConfig, writer, reader,computer);
		 
		try {
			if (instanceConfig.getReadParams().getHandler() != null) {
				if(instanceConfig.getReadParams().getHandler().startsWith(GlobalParam.GROUPID)) {
					reader.setReaderHandler((ReadHandler) Class.forName(instanceConfig.getReadParams().getHandler())
							.newInstance());
				}else {
					reader.setReaderHandler((ReadHandler) Class.forName(instanceConfig.getReadParams().getHandler(),true,GlobalParam.PLUGIN_CLASS_LOADER)
							.newInstance());
				}				
			}
			reader.setInstanceConfig(instanceConfig); 
			if(computer != null) {
				if (instanceConfig.getComputeParams().getHandler() != null) {
					if(instanceConfig.getComputeParams().getHandler().startsWith(GlobalParam.GROUPID)) {
						computer.setComputerHandler((ComputeHandler) Class.forName(instanceConfig.getComputeParams().getHandler())
								.newInstance());
					}else {
						computer.setComputerHandler((ComputeHandler) Class.forName(instanceConfig.getComputeParams().getHandler(),true,GlobalParam.PLUGIN_CLASS_LOADER)
								.newInstance());
					}				
				}
				computer.setInstanceConfig(instanceConfig);
			}			
			
			if (instanceConfig.getWriterParams().getHandler() != null) {
				for(WriterFlowSocket wfs : writer) {
					if(instanceConfig.getWriterParams().getHandler().startsWith(GlobalParam.GROUPID)) {
						wfs.setWriteHandler((WriteHandler) Class.forName(instanceConfig.getWriterParams().getHandler())
								.newInstance());
					}else {
						wfs.setWriteHandler((WriteHandler) Class.forName(instanceConfig.getWriterParams().getHandler(),true,GlobalParam.PLUGIN_CLASS_LOADER)
								.newInstance());
					}	
					wfs.setInstanceConfig(instanceConfig);
				}							
			}
		} catch (Exception e) {
			log.error("PipePump init Exception,", e);
		}
	}

	/**
	 * Job running entry
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
		String mainName = Common.getMainName(instance, L1seq);
		String writeTo = writeInSamePosition ? getInstanceConfig().getPipeParams().getInstanceName() : mainName;
		if (isFull) {
			job_type = JOB_TYPE.FULL;
		} else {
			job_type = JOB_TYPE.INCREMENT;
		}
		List<String> L2seqs = getInstanceConfig().getReadParams().getSeq().size() > 0
				? getInstanceConfig().getReadParams().getSeq()
				: Arrays.asList("");
		if (!Resource.FLOW_INFOS.containsKey(instance, job_type.name())) {
			Resource.FLOW_INFOS.set(instance, job_type.name(), new HashMap<String, String>());
		}
		Resource.FLOW_INFOS.get(instance, job_type.name()).put(instance + " seqs nums", String.valueOf(L2seqs.size()));
		Task task = Task.getInstance(instance, L1seq, job_type, getInstanceConfig(), null);
		processFlow(task, mainName, storeId, L2seqs, writeTo, writeInSamePosition);
		Resource.FLOW_INFOS.get(instance, job_type.name()).clear();
		if (isFull) {
			if (writeInSamePosition) {
				String destination = getInstanceConfig().getPipeParams().getInstanceName();
				synchronized (Resource.FLOW_INFOS.get(destination, GlobalParam.FLOWINFO.MASTER.name())) {
					String remainJobs = Resource.FLOW_INFOS.get(destination, GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_JOBS.name());
					remainJobs = remainJobs.replace(mainName, "").trim();
					Resource.FLOW_INFOS.get(destination, GlobalParam.FLOWINFO.MASTER.name())
							.put(GlobalParam.FLOWINFO.FULL_JOBS.name(), remainJobs);
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
	private void processFlow(Task task, String mainName, String storeId, List<String> L2seqs, String writeTo,
			boolean writeInSamePosition) throws EFException {
		for (String L2seq : L2seqs) {
			try {
				task.setL2seq(L2seq);
				Resource.FLOW_INFOS.get(task.getInstance(), task.getJobType().name()).put(task.getInstance() + L2seq,
						"start count page...");
				ConcurrentLinkedDeque<String> pageList = this.getPageLists(task);
				if (pageList == null)
					throw new EFException("Reader page split exception!", ELEVEL.Termination);
				processListsPages(task, writeTo, pageList, storeId);

			} catch (EFException e) {
				if (task.getJobType().equals(JOB_TYPE.FULL) && !writeInSamePosition) {
					for (int t = 0; t < 5; t++) {
						getWriter().PREPARE(false, false);
						if (getWriter().ISLINK()) {
							try {
								getWriter().removeInstance(mainName, storeId);
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
					log.error("[" + task.getJobType().name() + " " + mainName + L2seq + "_" + storeId + " ERROR]", e);
					Resource.mailSender.sendHtmlMailBySynchronizationMode(" [EFLOWS] " + GlobalParam.RUN_ENV,
							"Job " + mainName + " " + task.getJobType().name() + " Has stopped!");
				}
				Common.processErrorLevel(e);
			}
		}
	}

	private void processListsPages(Task task, String writeTo, ConcurrentLinkedDeque<String> pageList, String storeId)
			throws EFException {
		String mainName = Common.getMainName(task.getInstance(), task.getL1seq());
		int pageNum = pageList.size();
		if (pageNum == 0) {
			if(task.getInstanceConfig().getPipeParams().getLogLevel()==0)
				log.info(Common.formatLog("start", "Complete " + task.getJobType().name(), mainName, storeId,
						task.getL2seq(), 0, "", GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(task.getL2seq()), 0,
						" no data!"));
		} else {
			if(task.getInstanceConfig().getPipeParams().getLogLevel()<2)
				log.info(Common.formatLog("start",
						(getInstanceConfig().getPipeParams().isMultiThread() ? "MultiThread" : "SingleThread") + " Start "
								+ task.getJobType().name(),
						mainName, storeId, task.getL1seq(), 0, "",
						GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(task.getL2seq()), 0, ",totalpage:" + pageNum));
 
			long start = Common.getNow();
			AtomicInteger total = new AtomicInteger(0);
			if (getInstanceConfig().getPipeParams().isMultiThread()) {
				CountDownLatch synThreads = new CountDownLatch(estimateThreads(pageNum));
				Resource.ThreadPools.submitJobPage(
						new PumpThread(synThreads, task, storeId, pageList, writeTo, total));
				try {
					synThreads.await();
				} catch (Exception e) {
					throw new EFException(e);
				}
			} else {
				singleThread(task, storeId, pageList, writeTo, total);
			}
			if(task.getInstanceConfig().getPipeParams().getLogLevel()<2)
				log.info(Common.formatLog("complete", "Complete " + task.getJobType().name(), mainName, storeId,
						task.getL2seq(), total.get(), "",
						GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(task.getL2seq()), Common.getNow() - start, ""));
			if (Common.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination))
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
	private void singleThread(Task task, String storeId, ConcurrentLinkedDeque<String> pageList, String writeTo,
			 AtomicInteger total) throws EFException {
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
			Resource.FLOW_INFOS.get(task.getInstance(), task.getJobType().name())
					.put(task.getInstance() + task.getL2seq(), processPos + "/" + pageList.size());
			String dataScanDSL = PipeNormsUtil.fillParam(task.getScanParam().getDataScanDSL(),
					PipeNormsUtil.getScanParam(task.getL2seq(), startId, dataBoundary, task.getStartTime(),
							task.getEndTime(), scanField));
			if (Common.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination)) {
				break;
			} else {
				DataPage pagedata = this.getPageData(Page.getInstance(keyField, scanField, startId, dataBoundary,
						getInstanceConfig(), dataScanDSL));
				if (getInstanceConfig().openCompute()) {
					pagedata = (DataPage) CPU.RUN(getID(), "ML", "compute", false, getID(), task.getJobType().name(),
							writeTo, pagedata); 				
				} 	 
				rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
						writeTo, storeId, task.getL2seq(), pagedata, ",process:" + processPos + "/" + pageNum,
						isUpdate, false);
				if (rState.isStatus() == false)
					throw new EFException("writeDataSet data exception!");
				total.getAndAdd(rState.getCount());
				startId = dataBoundary;
			}

			if (rState.getReaderScanStamp().compareTo(GlobalParam.SCAN_POSITION.get(task.getInstance()).
					getL2SeqPos(task.getL2seq())) > 0) {
				GlobalParam.SCAN_POSITION.get(task.getInstance()).updateL2SeqPos(task.getL2seq(),
						rState.getReaderScanStamp());
			}
			if (task.getJobType() == JOB_TYPE.INCREMENT) {
				Common.saveTaskInfo(task.getInstance(), task.getL1seq(), storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
			}
		}
	}

	int estimateThreads(int numJobs) {
		return (int) (1 + Math.log(numJobs));
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
	public class PumpThread implements Runnable {
		long start = Common.getNow();
		final int pageSize;
		final String ID = CPU.getUUID();
		final String writeTo;
		final String storeId;
		final AtomicInteger total;

		Task task;
		CountDownLatch synThreads;
		ReaderState rState = null;
		AtomicInteger processPos = new AtomicInteger(0);
		String startId = "0";
		ConcurrentLinkedDeque<String> pageList;
		boolean isUpdate = getInstanceConfig().getPipeParams().getWriteType().equals("increment") ? true : false;

		public PumpThread(CountDownLatch synThreads, Task task, String storeId, ConcurrentLinkedDeque<String> pageList,
				String writeTo, AtomicInteger total) {
			this.pageList = pageList;
			this.writeTo = writeTo;
			this.storeId = storeId;
			this.synThreads = synThreads;
			this.pageSize = pageList.size();
			this.total = total;
			this.task = task;
		}

		public String getId() {
			return ID;
		}

		public int needThreads() {
			return estimateThreads(this.pageSize);
		}

		@Override
		public void run() {
			String dataBoundary;
			while (!pageList.isEmpty()) {
				dataBoundary = pageList.poll();
				processPos.incrementAndGet();
				Resource.FLOW_INFOS.get(task.getInstance(), task.getJobType().name())
						.put(task.getInstance() + task.getL2seq(), processPos + "/" + this.pageSize);
				String dataScanDSL = PipeNormsUtil.fillParam(task.getScanParam().getDataScanDSL(),
						PipeNormsUtil.getScanParam(task.getL2seq(), startId, dataBoundary, task.getStartTime(),
								task.getEndTime(), task.getScanParam().getScanField()));
				if (Common.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(),
						STATUS.Termination)) {
					Resource.ThreadPools.cleanWaitJob(getId());
					Common.LOG.warn(task.getInstance() + " " + task.getJobType().name() + " job has been Terminated!");
					break;
				} else {
					DataPage pagedata = getPageData(Page.getInstance(task.getScanParam().getKeyField(),
							task.getScanParam().getScanField(), startId, dataBoundary,
							getInstanceConfig(), dataScanDSL));
					if (getInstanceConfig().openCompute()) {
						pagedata = (DataPage) CPU.RUN(getID(), "ML", "compute", false, getID(),
								task.getJobType().name(), writeTo, pagedata);
				
					} 
					try {
						rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false,
								task.getJobType().name(), writeTo, storeId, task.getL2seq(), pagedata,
								",process:" + processPos + "/" + pageSize, isUpdate, false);
					} finally {
						Common.setFlowStatus(task.getInstance(), task.getL1seq(), GlobalParam.JOB_TYPE.FULL.name(),
								STATUS.Blank, STATUS.Ready,getInstanceConfig().getPipeParams().showInfoLog());
					}
					if (rState.isStatus() == false) {
						Common.LOG.warn("read data exception!");
						return;
					}
					total.addAndGet(rState.getCount());
					startId = dataBoundary;
				}

				if (rState.getReaderScanStamp().compareTo(
						GlobalParam.SCAN_POSITION.get(task.getInstance()).getL2SeqPos(task.getL2seq())) > 0) {
					GlobalParam.SCAN_POSITION.get(task.getInstance()).updateL2SeqPos(task.getL2seq(),
							rState.getReaderScanStamp());
				}
				if (task.getJobType() == JOB_TYPE.INCREMENT) {
					Common.saveTaskInfo(task.getInstance(), task.getL1seq(), storeId,
							GlobalParam.JOB_INCREMENTINFO_PATH);
				}
			}
			synThreads.countDown();
		}

	}

	public class tasks extends RecursiveTask<Integer> {

		private static final long serialVersionUID = -607427644538793287L;

		@Override
		protected Integer compute() {
			int total = 0;
			return total;
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
}
