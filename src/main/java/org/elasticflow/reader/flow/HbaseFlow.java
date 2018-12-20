package org.elasticflow.reader.flow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.handler.Handler;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
public class HbaseFlow extends ReaderFlowSocket { 
	 
	private final static Logger log = LoggerFactory.getLogger(HbaseFlow.class); 

	public static HbaseFlow getInstance(HashMap<String, Object> connectParams) {
		HbaseFlow o = new HbaseFlow();
		o.INIT(connectParams);
		return o;
	}

	@Override
	public void INIT(HashMap<String, Object> connectParams) {
		this.connectParams = connectParams;
		String tableColumnFamily = (String) this.connectParams
				.get("defaultValue");
		if (tableColumnFamily != null && tableColumnFamily.length() > 0) {
			String[] strs = tableColumnFamily.split(":");
			if (strs != null && strs.length > 0)
				this.connectParams.put("tableName", strs[0]);
			if (strs != null && strs.length > 1)
				this.connectParams.put("columnFamily", strs[1]);
		}
		this.poolName = String.valueOf(connectParams.get("poolName")); 
	}
 
	@Override
	public DataPage getPageData(HashMap<String, String> param,Map<String, RiverField> transParams,Handler handler,int pageSize) { 
		PREPARE(false,false);
		boolean releaseConn = false;
		try {
			if(!ISLINK())
				return this.dataPage;
			Table table = (Table) GETSOCKET().getConnection(true);
			Scan scan = new Scan();
			List<Filter> filters = new ArrayList<Filter>();
			SingleColumnValueFilter range = new SingleColumnValueFilter(
					Bytes.toBytes(this.connectParams.get("columnFamily")
							.toString()), Bytes.toBytes(param
							.get(GlobalParam._ScanField)),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(param.get("startTime"))));
			range.setLatestVersionOnly(true);
			range.setFilterIfMissing(true);
			filters.add(range);
			scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters));
			scan.setStartRow(Bytes.toBytes(param.get(GlobalParam._start)));
			scan.setStopRow(Bytes.toBytes(param.get(GlobalParam._end)));
			scan.setCaching(pageSize);
			scan.addFamily(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()));
			ResultScanner resultScanner = table.getScanner(scan);
			try {   
				String dataBoundary = null;
				String updateFieldValue=null; 
				this.dataPage.put(GlobalParam.READER_KEY, param.get(GlobalParam.READER_KEY));
				this.dataPage.put(GlobalParam.READER_SCAN_KEY, param.get(GlobalParam.READER_SCAN_KEY)); 
				for (Result r : resultScanner) { 
					PipeDataUnit u = PipeDataUnit.getInstance();
					if(handler==null){
						for (Cell cell : r.rawCells()) {
							String k = new String(CellUtil.cloneQualifier(cell));
							String v = new String(CellUtil.cloneValue(cell), "UTF-8"); 
							if(k.equals(this.dataPage.get(GlobalParam.READER_KEY))){
								u.setKeyColumnVal(v);
								dataBoundary = v;
							}
							if(k.equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))){
								updateFieldValue = v;
							}
							u.addFieldValue(k, v, transParams);
						} 
					}else{
						handler.handleData(r,u);
					} 
					this.dataUnit.add(u);
				} 
				if (updateFieldValue==null){ 
					this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis()); 
				}else{
					this.dataPage.put(GlobalParam.READER_LAST_STAMP, updateFieldValue); 
				}
				this.dataPage.putDataBoundary(dataBoundary);
				this.dataPage.putData(this.dataUnit);
			} catch (Exception e) {
				this.dataPage.put(GlobalParam.READER_LAST_STAMP, -1);
				log.error("SqlReader init Exception", e);
			} 
		} catch (Exception e) {
			releaseConn = true;
			log.error("get dataPage Exception", e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(HashMap<String, String> param,int pageSize) {
		int i = 0;
		ConcurrentLinkedDeque<String> dt = new ConcurrentLinkedDeque<>(); 
		PREPARE(false,false);
		if(!ISLINK())
			return dt; 
		boolean releaseConn = false;
		try {
			Scan scan = new Scan();
			Table table = (Table) GETSOCKET().getConnection(true);
			List<Filter> filters = new ArrayList<Filter>();
			SingleColumnValueFilter range = new SingleColumnValueFilter(
					Bytes.toBytes(this.connectParams.get("columnFamily")
							.toString()), Bytes.toBytes(param
							.get(GlobalParam._ScanField)),
					CompareFilter.CompareOp.GREATER_OR_EQUAL,
					new BinaryComparator(Bytes.toBytes(param.get("startTime"))));
			range.setLatestVersionOnly(true);
			range.setFilterIfMissing(true);
			filters.add(range);
			scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
					filters));
			scan.setCaching(pageSize);
			scan.addFamily(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()));
			scan.addColumn(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()), Bytes.toBytes(param
					.get(GlobalParam._ScanField)));
			scan.addColumn(Bytes.toBytes(this.connectParams.get("columnFamily")
					.toString()), Bytes.toBytes(param.get("column")));
			ResultScanner resultScanner = table.getScanner(scan);
			for (Result r : resultScanner) {
				if (i % pageSize == 0) {
					dt.add(Bytes.toString(r.getRow()));
				}
				i += r.size();
			}
		} catch (Exception e) {
			releaseConn = true;
			log.error("getPageSplit Exception", e);
		}finally{ 
			REALEASE(false,releaseConn);
		}
		return dt;
	} 

}
