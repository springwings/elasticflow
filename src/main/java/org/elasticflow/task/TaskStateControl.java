package org.elasticflow.task;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.JOB_TYPE;
import org.elasticflow.config.GlobalParam.STATUS;
import org.elasticflow.model.EFState;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.ScanPosition;
import org.elasticflow.node.CPU;
import org.elasticflow.piper.PipePump;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.EFDataStorer;
import org.elasticflow.util.instance.PipeUtil;
import org.elasticflow.yarn.Resource;
import org.elasticflow.yarn.coordinate.TaskStateCoord;

public class TaskStateControl implements TaskStateCoord{
	
	final static ConcurrentHashMap<String,ScanPosition> SCAN_POSITION = new ConcurrentHashMap<>(); 
	
	/**FLOW_STATUS store current flow running control status*/
	final static EFState<AtomicInteger> FLOW_STATUS = new EFState<>();
	
	public void setFlowStatus(String instance, String L1seq,String tag,AtomicInteger ai) {
		FLOW_STATUS.set(instance, L1seq, tag,ai);
	}
	
	public void setScanPosition(Task task,String scanStamp) {
		if (PipeUtil.scanPosCompare(scanStamp, SCAN_POSITION.get(task.getInstance())
				.getLSeqPos(Common.getLseq(task.getL1seq(), task.getL2seq())))) {
			SCAN_POSITION.get(task.getInstance())
			.updateLSeqPos(Common.getLseq(task.getL1seq(), task.getL2seq()),scanStamp);
		}		
	}
	
	public boolean checkFlowStatus(String instance,String seq,JOB_TYPE type,STATUS state) {
		if((FLOW_STATUS.get(instance, seq, type.name()).get() & state.getVal())>0)
			return true; 
		return false;
	} 
	
	/**
	 * 
	 * @param instance
	 * @param seq
	 * @param type tag for flow status,with job_type
	 * @param needState equal 0 no need check
	 * @param plusState
	 * @param removeState
	 * @return boolean,lock status
	 */
	public boolean setFlowStatus(String instance,String L1seq,String type,STATUS needState, STATUS setState,boolean showLog) {
		synchronized (FLOW_STATUS.get(instance, L1seq, type)) {
			if (needState.equals(STATUS.Blank) || (FLOW_STATUS.get(instance, L1seq, type).get() == needState.getVal())) {
				FLOW_STATUS.get(instance, L1seq, type).set(setState.getVal()); 
				return true;
			} else {
				if(showLog)
					Common.LOG.info("{} {} not in {} state!",instance,type,needState.name());
				return false;
			}
		}
	}
	
	/**
	 * get store tag name
	 * @param instanceName
	 * @param L1seq 
	 * @return String
	 */
	public String getStoreId(String instance, String L1seq,boolean reload) { 
		if(reload) {
			String path = Common.getTaskStorePath(instance, L1seq,GlobalParam.JOB_INCREMENTINFO_PATH);
			byte[] b = EFDataStorer.getData(path, true);
			if (b != null && b.length > 0) {
				String str = new String(b);
				SCAN_POSITION.put(instance, new ScanPosition(str,instance,L1seq));  
			}else {
				SCAN_POSITION.put(instance, new ScanPosition(instance,L1seq));
			}
		}  
		return SCAN_POSITION.get(instance).getStoreId();
	}
	
	public synchronized String getIncrementStoreId(String instance, String L1seq, PipePump transDataFlow,boolean reCompute) throws EFException {
		String storeId = getStoreId(instance,L1seq,true); 
		if (storeId.length() == 0 || reCompute) {
			storeId = (String) CPU.RUN(transDataFlow.getID(), "Pond", "getNewStoreId",false, Common.getInstanceId(instance, L1seq), true); 
			if (storeId == null)
				storeId = "a";
			saveTaskInfo(instance,L1seq,storeId,GlobalParam.JOB_INCREMENTINFO_PATH);
		}
		return storeId;
	}
	
	/**
	 * 
	 * @param instance
	 * @param L1seq
	 * @param storeId
	 * @param location
	 */
	public void saveTaskInfo(String instance, String L1seq,String storeId,String location) {
		SCAN_POSITION.get(instance).updateStoreId(storeId);
		EFDataStorer.setData(Common.getTaskStorePath(instance, L1seq,location),
				SCAN_POSITION.get(instance).getString());
	}  
	
	/**
	 * for Master/slave job get and set LastUpdateTime
	 * @param instance
	 * @param L1seq
	 * @param storeId  Master store id
	 */
	public void setAndGetScanInfo(String instance, String L1seq,String storeId) {
		synchronized (SCAN_POSITION) {
			if(!SCAN_POSITION.containsKey(instance)) {
				String path = Common.getTaskStorePath(instance, L1seq,GlobalParam.JOB_INCREMENTINFO_PATH);
				byte[] b = EFDataStorer.getData(path,true);
				if (b != null && b.length > 0) {
					String str = new String(b); 
					SCAN_POSITION.put(instance, new ScanPosition(str,instance,storeId));  
				}else {
					SCAN_POSITION.put(instance, new ScanPosition(instance,storeId));
				}
				saveTaskInfo(instance, L1seq,storeId,GlobalParam.JOB_INCREMENTINFO_PATH);
			}
		}
		
	}

	/**
	 * get increment store tag name and will auto create new one with some conditions.
	 * 
	 * @param isIncrement
	 * @param reCompute
	 *            force to get storeid recompute from destination engine
	 * @param L1seq
	 *            for series data source sequence
	 * @param instance
	 *            data source main tag name
	 * @return String
	 * @throws EFException 
	 */
	public String getStoreId(String instance, String L1seq, PipePump transDataFlow, boolean isIncrement,
			boolean reCompute){
		try {
			if (isIncrement) { 
				return getIncrementStoreId(instance,L1seq,transDataFlow,reCompute);
			} else {
				return (String) CPU.RUN(transDataFlow.getID(), "Pond", "getNewStoreId",true, Common.getInstanceId(instance, L1seq), false);
			}
		} catch (EFException e) {
			Common.LOG.error("getStoreId",e);
			Resource.FlOW_CENTER.removeInstance(instance, true, true);
			Common.processErrorLevel(e);
		}
		return null;		
	} 
	
	public void scanPositionkeepCurrentPos(String instance) {
		SCAN_POSITION.get(instance).keepCurrentPos();
	}
	
	public void scanPositionRecoverKeep(String instance) {
		SCAN_POSITION.get(instance).recoverKeep();
	}
	
	public String getscanPositionString(String instance) {
		return SCAN_POSITION.get(instance)
				.getPositionString();
	}
	
	public void putScanPosition(String instance,ScanPosition scanPosition) {
		SCAN_POSITION.put(instance, scanPosition);
	}
	
	public String getLSeqPos(String instance,String L1seq,String L2seq) {
		return SCAN_POSITION.get(instance).getLSeqPos(Common.getLseq(L1seq, L2seq));
	}
	
	public void updateLSeqPos(String instance,String L1seq,String L2seq,String position){
		SCAN_POSITION.get(instance).updateLSeqPos(
				Common.getLseq(L1seq, L2seq), position);  
	}
	
	public void batchUpdateSeqPos(String instance,String val) {
		SCAN_POSITION.get(instance).batchUpdateSeqPos(val);
	}
	
	/**
	 * from SCAN_POSITION get store id
	 * @param instance
	 * @return
	 */
	public String getStoreId(String instance) {
		return SCAN_POSITION.get(instance).getStoreId();
	}
}
