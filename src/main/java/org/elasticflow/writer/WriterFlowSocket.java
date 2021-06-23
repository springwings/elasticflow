package org.elasticflow.writer;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.field.EFField;
import org.elasticflow.flow.Flow;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFTuple;
import org.elasticflow.util.EFWriterUtil;
 
/**
 * Flow into Pond Manage
 * @author chengwen
 * @version 4.0
 * @date 2018-11-14 16:54
 * 
 */
@NotThreadSafe
public abstract class WriterFlowSocket extends Flow{
	
	/**batch submit documents*/
	protected Boolean isBatch = true;   
	
	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams;
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
		this.isBatch = GlobalParam.WRITE_BATCH; 
	}   
	
	/**
	 * Get the time-stamp of 0 o'clock every day/month
	 * Maintain expired instances
	 * @param mainName
	 * @param isIncrement
	 * @param instanceConfig
	 * @return time-stamp of second
	 */
	protected String timeMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		EFTuple<Long, Long> dTuple = EFWriterUtil.timeMechanism(instanceConfig);
		//remove out of range instance		 
		String iName = Common.getStoreName(mainName, String.valueOf(dTuple.v2));
		try {
			this.removeInstance(mainName, String.valueOf(dTuple.v2));
		} catch (Exception e) {
			Common.LOG.error("remove instance　"+iName+" Exception!", e);
		}		
		return String.valueOf(dTuple.v1);
	}
	
	/**Create storage node*/
	public abstract boolean create(String instance, String storeId, InstanceConfig instanceConfig);
	
	public abstract String getNewStoreId(String mainName,boolean isIncrement,InstanceConfig instanceConfig);
	
	/**write one row data **/
	public abstract void write(WriterParam writerParam,PipeDataUnit unit,Map<String, EFField> transParams,String instance, String storeId,boolean isUpdate) throws EFException;
	
	/**Delete a single record through the key id*/ 
	public abstract void delete(String instance, String storeId,String keyColumn,String keyVal) throws EFException;
  
	public abstract void removeInstance(String instance, String storeId);
	
	public abstract void setAlias(String instance, String storeId, String aliasName);

	public abstract void flush() throws Exception;

	public abstract void optimize(String instance, String storeId);
}
