package org.elasticflow.reader.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;

/**
 * Hbase database reader mainly consists of two parts: pagination query and detailed content query
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
public class HbaseReader extends ReaderFlowSocket { 
 
	private String columnFamily;
	
	final String DEFAULT_KEY = "tableColumnFamily"; 

	public static HbaseReader getInstance(ConnectParams connectParams) {
		HbaseReader o = new HbaseReader();
		o.INIT(connectParams);
		return o;
	}
 
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams;
		String tableColumnFamily = connectParams.getWhp().getDefaultValue().getString(DEFAULT_KEY);
		if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
			String[] strs = tableColumnFamily.split(":"); 
			if (strs != null && strs.length > 1)
				this.columnFamily = strs[1];
		}
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq()); 
	}
 
	@Override
	public DataPage getPageData(final TaskCursor taskCursor,int pageSize) throws EFException { 
		PREPARE(false,false);
		boolean clearConn = false;
		try {
			if(!connStatus())
				return this.dataPage;
			Table table = (Table) GETSOCKET().getConnection(END_TYPE.reader);
			Scan scan = new Scan();
			List<Filter> filters = new ArrayList<Filter>();
			SingleColumnValueFilter range = new SingleColumnValueFilter(
					Bytes.toBytes(this.columnFamily), Bytes.toBytes(taskCursor.getReaderScanKey()),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(taskCursor.getStart())));
			range.setLatestVersionOnly(true);
			range.setFilterIfMissing(true);
			filters.add(range);
			scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters));
			scan.setStartRow(Bytes.toBytes(taskCursor.getStart()));
			scan.setStopRow(Bytes.toBytes(taskCursor.getEnd()));
			scan.setCaching(pageSize);
			scan.addFamily(Bytes.toBytes(this.columnFamily));
			ResultScanner resultScanner = table.getScanner(scan);
			try {   
				String dataBoundary = null;
				String updateFieldValue=null; 
				this.dataPage.put(GlobalParam.READER_KEY, taskCursor.getReaderKey());
				this.dataPage.put(GlobalParam.READER_SCAN_KEY, taskCursor.getReaderScanKey()); 
				if(this.readHandler!=null && this.readHandler.supportHandleData()){
					this.readHandler.handleData(this,resultScanner,taskCursor,pageSize);
				}else {
					for (Result r : resultScanner) { 
						PipeDataUnit u = PipeDataUnit.getInstance();
						for (Cell cell : r.rawCells()) {
							String k = new String(CellUtil.cloneQualifier(cell));
							String v = new String(CellUtil.cloneValue(cell), "UTF-8"); 
							if(k.equals(this.dataPage.get(GlobalParam.READER_KEY))){
								u.setReaderKeyVal(v);
								dataBoundary = v;
							}
							if(k.equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))){
								updateFieldValue = v;
							}
							PipeDataUnit.addFieldValue(k, v, taskCursor.getInstanceConfig().getReadFields(),u);
						} 
						this.dataUnit.add(u);
					} 
				}
				
				if (updateFieldValue==null){ 
					this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis()); 
				}else{
					this.dataPage.put(GlobalParam.READER_LAST_STAMP, updateFieldValue); 
				}
				this.dataPage.putDataBoundary(dataBoundary);
				this.dataPage.putData(this.dataUnit);
			} catch (Exception e) {
				clearConn = true;
				this.dataPage.put(GlobalParam.READER_LAST_STAMP, -1);
				throw new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " Hbase Reader get dataPage Exception!"); 
			} 
		} catch (Exception e) {
			clearConn = true; 
			throw new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " Hbase Reader get dataPage Exception!");
		}finally{
			releaseConn(false,clearConn);
		} 
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task,int pageSize) throws EFException {
		int i = 0;
		ConcurrentLinkedDeque<String> dt = new ConcurrentLinkedDeque<>(); 
		PREPARE(false,false);
		if(!connStatus())
			return dt; 
		boolean clearConn = false;
		try {
			Scan scan = new Scan();
			Table table = (Table) GETSOCKET().getConnection(END_TYPE.reader);
			List<Filter> filters = new ArrayList<Filter>();
			SingleColumnValueFilter range = new SingleColumnValueFilter(
					Bytes.toBytes(this.columnFamily), Bytes.toBytes(task.getScanParam().getScanField()),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(task.getStartTime())));
			range.setLatestVersionOnly(true);
			range.setFilterIfMissing(true);
			filters.add(range);
			scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters));
			scan.setCaching(pageSize);
			scan.addFamily(Bytes.toBytes(this.columnFamily));
			scan.addColumn(Bytes.toBytes(this.columnFamily), Bytes.toBytes(task.getScanParam().getScanField()));
 			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				if (i % pageSize == 0) {
					dt.add(Bytes.toString(r.getRow()));
				}
				i += r.size();
			}
		} catch (Exception e) {
			clearConn = true;
			throw new EFException(e,task.getInstanceID()+ " Hbase Reader get page lists Exception!");  
		}finally{ 
			releaseConn(false,clearConn);
		}
		return dt;
	}

}
