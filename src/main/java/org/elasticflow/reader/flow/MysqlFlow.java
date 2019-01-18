package org.elasticflow.reader.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.field.RiverField;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.FNException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */
public class MysqlFlow extends ReaderFlowSocket{    

	private final static Logger log = LoggerFactory.getLogger(MysqlFlow.class);  

	public static MysqlFlow getInstance(final ConnectParams connectParams) {
		MysqlFlow o = new MysqlFlow();
		o.INIT(connectParams);
		return o;
	}  
 
	@Override
	public DataPage getPageData(final Page page,int pageSize) {  
		boolean releaseConn = false;
		PREPARE(false,false); 
		if(!ISLINK())
			return this.dataPage; 
		Connection conn = (Connection) GETSOCKET().getConnection(false); 
		try (PreparedStatement statement = conn.prepareStatement(page.getAdditional());){ 
			statement.setFetchSize(pageSize); 
			try(ResultSet rs = statement.executeQuery();){				
				this.dataPage.put(GlobalParam.READER_KEY, page.getReaderKey());
				this.dataPage.put(GlobalParam.READER_SCAN_KEY, page.getReaderScanKey());
				if(page.getReadHandler()==null){
					getAllData(rs,page.getTransField()); 
				}else{
					page.getReadHandler().handleData(this,rs,page.getTransField());
				} 
			} catch (Exception e) {
				this.dataPage.put(GlobalParam.READER_STATUS,false);
				log.error("get data Page Exception", e);
			} 
		} catch (SQLException e){
			this.dataPage.put(GlobalParam.READER_STATUS,false);
			log.error(page.getAdditional() + " get dataPage SQLException", e);
		} catch (Exception e) { 
			releaseConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS,false);
			log.error("get dataPage Exception so free connection,details ", e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return this.dataPage;
	} 
	
	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize) {
		String sql;
		if(task.getScanParam().getPageScanDSL()!=null){
			sql = " select "+GlobalParam._page_field+" as id,(@a:=@a+1) AS FN_ROW_ID from ("
					+ task.getScanParam().getPageScanDSL()
					+ ") FN_FPG_MAIN join (SELECT @a := -1) FN_FPG_ROW order by "+GlobalParam._page_field+" desc";
		}else{
			sql = " select "+GlobalParam._page_field+" as id,(@a:=@a+1) AS FN_ROW_ID from ("
					+ task.getScanParam().getDataScanDSL()
					+ ") FN_FPG_MAIN join (SELECT @a := -1) FN_FPG_ROW order by "+GlobalParam._page_field+" desc"; 
		}
		sql = " select id from (" + sql
				+ ") FN_FPG_END where MOD(FN_ROW_ID, "+pageSize+") = 0";
		sql = sql 
				.replace(GlobalParam._scan_field, task.getScanParam().getScanField())
				.replace(GlobalParam._page_field, task.getScanParam().getPageField())
				.replace(GlobalParam._start_time, task.getStartTime())
				.replace(GlobalParam._end_time, task.getEndTime());
		if (task.getL2seq() != null && task.getL2seq().length() > 0)
			sql = sql.replace(GlobalParam._seq, task.getL2seq()); 
		 
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		PREPARE(false,false); 
		if(!ISLINK())
			return page;
		Connection conn = (Connection) GETSOCKET().getConnection(false); 
		PreparedStatement statement = null;
		ResultSet rs  = null;
		boolean releaseConn = false;
		try {
			boolean autoSelect = true; 
			if(task.getScanParam().getKeyFieldType() != null){
				autoSelect = false;
				if(task.getScanParam().getKeyFieldType().equals("int")){
					statement = conn.prepareStatement(sql.replace("#{end}", Long.MAX_VALUE + "").replace(
							"#{END}", Long.MAX_VALUE + ""));
				}else{
					statement = conn.prepareStatement(sql.replace("#{end}", "~").replace("#{END}", "~")); 
				}
			}else{
				statement = conn.prepareStatement(sql.replace("#{end}", Long.MAX_VALUE + "").replace(
						"#{END}", Long.MAX_VALUE + ""));
			} 
			statement.setFetchSize(pageSize);
			rs = statement.executeQuery(); 
			while (rs.next()) { 
				page.push(rs.getString("id"));
			}
			if (autoSelect && page.size() == 0) {
				statement.close();
				rs.close();
				statement = conn.prepareStatement(sql.replace("#{end}", "~").replace("#{END}", "~")); 
				rs = statement.executeQuery();  
				while (rs.next()) {
					page.push(rs.getString("id"));
				}
			} 
		}catch(SQLException e){
			page = null;
			log.error("get dataPage SQLException "+sql, e);
		}catch (Exception e) {
			releaseConn = true;
			page = null;
			log.error("get dataPage Exception so free connection,details ", e);
		}finally{ 
			try {
				statement.close();
				rs.close();
			} catch (Exception e) {
				log.error("close connection resource Exception", e);
			} 
			REALEASE(false,releaseConn);  
		}  
		return page;
	} 
	
	private void getAllData(ResultSet rs,Map<String, RiverField> transParam) throws FNException {   
		String dataBoundary = null;
		String LAST_STAMP=null;
		try {  
			ResultSetMetaData metaData = rs.getMetaData();
			int columncount = metaData.getColumnCount(); 
			while (rs.next()) {
				PipeDataUnit u = PipeDataUnit.getInstance();
				for (int i = 1; i < columncount + 1; i++) {
					String v = rs.getString(i);
					String k = metaData.getColumnLabel(i);
					if(k.equals(this.dataPage.get(GlobalParam.READER_KEY))){
						u.setKeyColumnVal(v);
						dataBoundary = v;
					}
					if(k.equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))){
						LAST_STAMP = v;
					}
					u.addFieldValue(k, v, transParam);
				}
				this.dataUnit.add(u);
			}
			rs.close();
		} catch (Exception e) {
			throw new FNException(e.getMessage());
		}
		if (LAST_STAMP==null){ 
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis()); 
		}else{
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, LAST_STAMP); 
		}
		this.dataPage.putDataBoundary(dataBoundary);
		this.dataPage.putData(this.dataUnit);
	} 
}