package org.elasticflow.piper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.elasticflow.reader.handler.Handler;
import org.elasticflow.util.Common;
import org.elasticflow.util.FNException;
import org.elasticflow.util.SqlUtil;
import org.elasticflow.writer.WriterFlowSocket;
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
	/** defined custom read flow socket */
	Handler readHandler;

	public static PipePump getInstance(ReaderFlowSocket reader, WriterFlowSocket writer,
			InstanceConfig instanceConfig) {
		return new PipePump(reader, writer, instanceConfig);
	}

	private PipePump(ReaderFlowSocket reader, WriterFlowSocket writer, InstanceConfig instanceConfig) {
		CPU.prepare(getID(), instanceConfig, writer, reader);
		try {
			if (instanceConfig.getPipeParams().getReadHandler() != null) {
				this.readHandler = (Handler) Class.forName(instanceConfig.getPipeParams().getReadHandler())
						.newInstance();
			}
		} catch (Exception e) {
			log.error("PipePump init Exception,", e);
		}
	}

	public void run(String instance, String storeId, String L1seq, boolean isFull, boolean masterControl)
			throws FNException {
		JOB_TYPE job_type;
		String mainName = Common.getMainName(instance, L1seq);
		String writeTo = masterControl ? getInstanceConfig().getPipeParams().getInstanceName() : mainName;
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

		if (getInstanceConfig().getReadParams().isNoSql()) {
			noSqlFlow(instance, mainName, job_type, isFull, storeId, L1seq, L2seqs, writeTo, masterControl); 
		} else {
			sqlFlow(instance, mainName, job_type, isFull, storeId, L1seq, L2seqs, writeTo, masterControl);
		}
		Resource.FLOW_INFOS.get(instance, job_type.name()).clear();
		if (isFull) {
			if (masterControl) {
				String _dest = getInstanceConfig().getPipeParams().getInstanceName();
				synchronized (Resource.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())) {
					String remainJobs = Resource.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
							.get(GlobalParam.FLOWINFO.FULL_JOBS.name());
					remainJobs = remainJobs.replace(mainName, "").trim();
					Resource.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
							.put(GlobalParam.FLOWINFO.FULL_JOBS.name(), remainJobs);
					if (remainJobs.length() == 0) {
						String _storeId = Resource.FLOW_INFOS.get(_dest, GlobalParam.FLOWINFO.MASTER.name())
								.get(GlobalParam.FLOWINFO.FULL_STOREID.name());
						PipePump ts = Resource.SOCKET_CENTER.getPipePump(_dest, null, false,
								GlobalParam.FLOW_TAG._DEFAULT.name());
						CPU.RUN(ts.getID(), "Pond", "switchInstance", true, instance, L1seq, _storeId);
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

	public WriterFlowSocket getWriter() {
		return CPU.getContext(getID()).getWriter();
	}

	/**
	 * write to not db platform
	 * 
	 * @param instanceName
	 * @param storeId
	 * @param L1seq
	 * @param isFullIndex
	 * @param masterControl
	 * @throws FNException
	 */
	private void noSqlFlow(String instance, String mainName, JOB_TYPE job_type, boolean isFull, String storeId,
			String L1seq, List<String> L2seqs, String writeTo, boolean masterControl) throws FNException { 
		Task task = Task.getInstance(instance, L1seq, job_type, getInstanceConfig().getReadParams(), null, this.readHandler); 
		for (String L2seq : L2seqs) {
			try {
				task.setL2seq(L2seq);  
				Resource.FLOW_INFOS.get(instance, job_type.name()).put(instance + L2seq, "start count page...");
				getReader().lock.lock();
				ConcurrentLinkedDeque<String> pageList = getReader().getPageSplit(task,
						getInstanceConfig().getPipeParams().getReadPageSize());
				getReader().lock.unlock();
				if (pageList == null)
					throw new FNException("read data get page split exception!");
				int pageNum = pageList.size();
				if (pageNum == 0) {
					log.info(Common.formatLog("start", "Complete " + job_type.name(), mainName, storeId, L2seq, 0, "",
							GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(L2seq), 0, " no data!"));
				} else {
					long start = Common.getNow();
				}
			} catch (Exception e) {

			}
		}
	}

	/**
	 * do Sql resource data job
	 * 
	 * @param instance
	 * @param mainName
	 * @param job_type
	 * @param isFull
	 * @param storeId
	 * @param L1seq
	 * @param L2seqs
	 * @param writeTo
	 * @param masterControl
	 * @throws FNException
	 */
	private void sqlFlow(String instance, String mainName, JOB_TYPE job_type, boolean isFull, String storeId,
			String L1seq, List<String> L2seqs, String writeTo, boolean masterControl) throws FNException { 
		Task task = Task.getInstance(instance, L1seq, job_type, getInstanceConfig().getReadParams(), null, this.readHandler);
		for (String L2seq : L2seqs) {
			try {
				task.setL2seq(L2seq); 
				Resource.FLOW_INFOS.get(instance, job_type.name()).put(instance + L2seq, "start count page...");
				getReader().lock.lock();
				ConcurrentLinkedDeque<String> pageList = getReader().getPageSplit(task,
						getInstanceConfig().getPipeParams().getReadPageSize());
				getReader().lock.unlock();
				if (pageList == null)
					throw new FNException("read data get page split exception!");
				processListsPages(task, writeTo, pageList,storeId);

			} catch (Exception e) {
				if (isFull && !masterControl) {
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
				if (e.getMessage() != null && e.getMessage().equals("storeId not found")) {
					throw new FNException("storeId not found");
				} else {
					log.error("[" + job_type.name() + " " + mainName + L2seq + "_" + storeId + " ERROR]", e);
					Resource.mailSender.sendHtmlMailBySynchronizationMode(" [Rivers] " + GlobalParam.run_environment,
							"Job " + mainName + " " + job_type.name() + " Has stopped!");
				}
			}
		}
	}

	private void processListsPages(Task task, String writeTo, ConcurrentLinkedDeque<String> pageList,
			String storeId) throws Exception {
		String mainName = Common.getMainName(task.getInstance(), task.getL1seq());
		int pageNum = pageList.size();
		if (pageNum == 0) {
			log.info(Common.formatLog("start", "Complete " + task.getJobType().name(), mainName, storeId, task.getL2seq(), 0, "",
					GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(task.getL2seq()), 0, " no data!"));
		} else {
			log.info(Common.formatLog("start",
					getInstanceConfig().getPipeParams().isMultiThread() ? "MultiThread" : "SingleThread" + " Start " + task.getJobType().name(),
					mainName, storeId, task.getL1seq(), 0, "", GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(task.getL2seq()), 0,
					",totalpage:" + pageNum));
			long start = Common.getNow();
			AtomicInteger total = new AtomicInteger(0);
			if (getInstanceConfig().getPipeParams().isMultiThread()) {
				final CountDownLatch synThreads = new CountDownLatch(estimateThreads(pageNum));
				Resource.ThreadPools.submitJobPage(new Pump(synThreads, task, storeId, pageList, writeTo, total));
				synThreads.await();
			} else {
				singleThread(task, storeId, pageList, writeTo, total);
			}
			log.info(Common.formatLog("complete", "Complete " + task.getJobType().name(), mainName, storeId, task.getL2seq(), total.get(),
					"", GlobalParam.SCAN_POSITION.get(mainName).getL2SeqPos(task.getL2seq()), Common.getNow() - start, ""));
			if (Common.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination))
				throw new FNException(task.getInstance() + " " + task.getJobType().name() + " job has been Terminated!");
		}
	} 
	

	/**
	 * use single thread process task, it is a safe mode support recover mechanism
	 * 
	 * @param pageList
	 * @throws FNException
	 */
	private void singleThread(Task task, String storeId, ConcurrentLinkedDeque<String> pageList, String writeTo, AtomicInteger total) throws FNException {
		ReaderState rState = null;
		int processPos = 0;
		String startId = "0";

		boolean isUpdate = getInstanceConfig().getPipeParams().getWriteType().equals("increment") ? true : false;
		String scanField = task.getScanParam().getScanField();
		String keyField = task.getScanParam().getKeyField();
		String dataBoundary;
		int pageNum = pageList.size();
		while (!pageList.isEmpty()) {
			dataBoundary = pageList.poll();
			processPos++;
			Resource.FLOW_INFOS.get(task.getInstance(), task.getJobType().name()).put(task.getInstance() + task.getL2seq(),
					processPos + "/" + pageList.size());
			String sql = SqlUtil.fillParam(task.getScanParam().getDataScanDSL(), SqlUtil.getScanParam(task.getL2seq(), startId, dataBoundary,
					task.getStartTime(), task.getEndTime(), scanField));
			if (Common.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination)) {
				break;
			} else {
				getReader().lock.lock();
				DataPage pagedata = (DataPage) CPU.RUN(
						getID(), "Pipe", "fetchPage", false, Page.getInstance(keyField, scanField, startId,
								dataBoundary, this.readHandler, getInstanceConfig().getWriteFields(), sql),
						getReader());
				getReader().freeJobPage();
				getReader().lock.unlock();
				if (getInstanceConfig().openCompute()) {
					pagedata = (DataPage) CPU.RUN(getID(), "ML", "computeDataSet", false, getID(), task.getJobType().name(),
							writeTo, pagedata);
					if (processPos == pageNum) {
						rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(), writeTo,
								storeId, task.getL2seq(), pagedata, ",process:" + processPos + "/" + pageNum, isUpdate, false);
					} else {
						continue;
					}
				} else {
					rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(), writeTo,
							storeId, task.getL2seq(), pagedata, ",process:" + processPos + "/" + pageNum, isUpdate, false);
				}
				if (rState.isStatus() == false)
					throw new FNException("writeDataSet data exception!");
				total.getAndAdd(rState.getCount());
				startId = dataBoundary;
			}

			if ((this.readHandler == null || !this.readHandler.loopScan(task)) && rState.getReaderScanStamp()
					.compareTo(GlobalParam.SCAN_POSITION.get(task.getInstance()).getL2SeqPos(task.getL2seq())) > 0) {
				GlobalParam.SCAN_POSITION.get(task.getInstance()).updateL2SeqPos(task.getL2seq(), rState.getReaderScanStamp());
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
	public class Pump implements Runnable {
		long start = Common.getNow();
		final int pageSize;
		final String ID = CPU.getUUID();
		CountDownLatch synThreads;

		boolean isUpdate = getInstanceConfig().getPipeParams().getWriteType().equals("increment") ? true : false;
		ReaderState rState = null;
		AtomicInteger processPos = new AtomicInteger(0);
		String startId = "0";
		final AtomicInteger total;
  
		Task task;
		ConcurrentLinkedDeque<String> pageList; 
		String writeTo;
		String storeId;

		public Pump(CountDownLatch synThreads, Task task, String storeId, ConcurrentLinkedDeque<String> pageList, String writeTo, AtomicInteger total) {
			this.pageList = pageList; 
			this.writeTo = writeTo;
			this.storeId = storeId;
			this.synThreads = synThreads;
			this.pageSize = pageList.size();
			this.total = total;
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
				Resource.FLOW_INFOS.get(task.getInstance(), task.getJobType().name()).put(task.getInstance() + task.getL2seq(),
						processPos + "/" + this.pageSize);
				String sql = SqlUtil.fillParam(task.getScanParam().getDataScanDSL(), SqlUtil.getScanParam(task.getL2seq(), startId, dataBoundary,
						task.getStartTime(), task.getEndTime(), task.getScanParam().getScanField()));
				if (Common.checkFlowStatus(task.getInstance(), task.getL1seq(), task.getJobType(), STATUS.Termination)) {
					Resource.ThreadPools.cleanWaitJob(getId());
					Common.LOG.warn(task.getInstance() + " " + task.getJobType().name() + " job has been Terminated!");
					break;
				} else {
					getReader().lock.lock();
					DataPage pagedata = (DataPage) CPU.RUN(getID(), "Pipe", "fetchPage", false,
							Page.getInstance(task.getScanParam().getKeyField(), task.getScanParam().getScanField(), startId, dataBoundary, readHandler,
									getInstanceConfig().getWriteFields(), sql),
							getInstanceConfig().getWriteFields(), getReader());
					getReader().freeJobPage();
					getReader().lock.unlock();
					if (getInstanceConfig().openCompute()) {
						pagedata = (DataPage) CPU.RUN(getID(), "ML", "computeDataSet", false, getID(), task.getJobType().name(),
								writeTo, pagedata);
						synchronized (processPos) {
							if (processPos.get() == this.pageSize) {
								rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
										writeTo, storeId, task.getL2seq(), pagedata,
										",process:" + processPos + "/" + this.pageSize, isUpdate, false);
							} else {
								continue;
							}
						}
					} else {
						try {
							rState = (ReaderState) CPU.RUN(getID(), "Pipe", "writeDataSet", false, task.getJobType().name(),
									writeTo, storeId, task.getL2seq(), pagedata, ",process:" + processPos + "/" + pageSize,
									isUpdate, false);
						} finally {
							Common.setFlowStatus(task.getInstance(), task.getL1seq(), GlobalParam.JOB_TYPE.FULL.name(), STATUS.Blank,
									STATUS.Ready);
						}
					}
					if (rState.isStatus() == false) {
						Common.LOG.warn("read data exception!");
						return;
					}
					total.addAndGet(rState.getCount());
					startId = dataBoundary;
				}

				if ((readHandler == null || !readHandler.loopScan(task)) && rState.getReaderScanStamp()
						.compareTo(GlobalParam.SCAN_POSITION.get(task.getInstance()).getL2SeqPos(task.getL2seq())) > 0) {
					GlobalParam.SCAN_POSITION.get(task.getInstance()).updateL2SeqPos(task.getL2seq(), rState.getReaderScanStamp());
				}
				if (task.getJobType() == JOB_TYPE.INCREMENT) {
					Common.saveTaskInfo(task.getInstance(), task.getL1seq(), storeId, GlobalParam.JOB_INCREMENTINFO_PATH);
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
}
