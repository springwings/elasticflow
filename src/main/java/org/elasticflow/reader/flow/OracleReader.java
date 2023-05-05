package org.elasticflow.reader.flow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.concurrent.ThreadSafe;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.model.Page;
import org.elasticflow.model.Task;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle database reader mainly consists of two parts: pagination query and detailed content query
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */

@ThreadSafe
public class OracleReader extends ReaderFlowSocket{ 
  
	private final static Logger log = LoggerFactory.getLogger(OracleReader.class);

	public static OracleReader getInstance(final ConnectParams connectParams) {
		OracleReader o = new OracleReader();
		o.initConn(connectParams);
		return o;
	} 
 

	@Override
	public DataPage getPageData(final Page page,int pageSize) throws EFException {
		boolean releaseConn = false;
		PREPARE(false,false, false); 
		if(!ISLINK())
			return this.dataPage; 
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.reader); 
		try (PreparedStatement statement = conn.prepareStatement(page.getAdditional());){ 
			statement.setFetchSize(pageSize); 
			try(ResultSet rs = statement.executeQuery();){				
				this.dataPage.put(GlobalParam.READER_KEY, page.getReaderKey());
				this.dataPage.put(GlobalParam.READER_SCAN_KEY, page.getReaderScanKey());
				if(this.readHandler!=null && this.readHandler.supportHandleData()){
					//handler reference getAllData function 
					this.readHandler.handleData(this,rs,page,pageSize);					
				}else{
					getAllData(rs,page.getInstanceConfig().getReadFields()); 
				} 
				this.dataPage.put(GlobalParam.READER_STATUS,true);
			} catch (Exception e) {
				throw new EFException(e);
			} 
		} catch (SQLException e){  
			EFException err = new EFException(e);
			err.track(page.getAdditional());
			throw err;
		} catch (Exception e) { 
			releaseConn = true;
			log.error("get dataPage Exception will auto free connection!");
			throw new EFException(e);
		}finally{
			REALEASE(false,releaseConn);
		} 
		return this.dataPage;
	}

	@Override
	public ConcurrentLinkedDeque<String> getPageSplit(final Task task,int pageSize) throws EFException {
		String sql;
		if(task.getScanParam().getPageScanDSL()!=null){
			sql = " select "+GlobalParam._page_field+" as id,ROWNUM AS EF_ROW_ID from ("
					+ task.getScanParam().getPageScanDSL()
					+ ") EF_FPG_MAIN  order by "+GlobalParam._page_field+" desc";
		}else{
			sql = " select "+GlobalParam._page_field+" as id,ROWNUM AS EF_ROW_ID from ("
					+ task.getScanParam().getDataScanDSL()
					+ ") EF_FPG_MAIN  order by "+GlobalParam._page_field+" desc";
		} 
		sql = " select id from (" + sql + ") EF_FPG_END where MOD(EF_ROW_ID, "
				+ pageSize + ") = 0";
		sql = sql 
				.replace(GlobalParam._scan_field, task.getScanParam().getScanField())
				.replace(GlobalParam._page_field, task.getScanParam().getPageField())
				.replace(GlobalParam._start_time, task.getStartTime())
				.replace(GlobalParam._end_time, task.getEndTime());
		if (task.getL2seq() != null && task.getL2seq().length() > 0)
			sql = sql.replace(GlobalParam._seq, task.getL2seq()); 
		 
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		PREPARE(false,false, false); 
		if(!ISLINK())
			return page;
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.reader);
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
					page.add(rs.getString("id"));
				}
			} 
		}catch(SQLException e){
			page = null;
			EFException err = new EFException(e);
			err.track(sql);
			throw err;
		}catch (Exception e) {
			releaseConn = true;
			page = null;
			log.error("{} Oracle Reader get page lists Exception, system will auto free connection!",task.getInstanceID(),e);
			throw new EFException("Oracle Reader get page lists Exception");			
		}finally{ 
			try {
				if(statement!=null && rs!=null) {
					statement.close();
					rs.close();
				}
			} catch (Exception e) {
				releaseConn = true; 
				log.error("close connection resource Exception!",e);
				throw new EFException("Oracle close connection Exception!");	
			} 
			REALEASE(false,releaseConn);  
		}  
		return page;
	} 
	 

	private void getAllData(ResultSet rs,Map<String, EFField> transParam) throws EFException {   
		String dataBoundary = null;
		String LAST_STAMP = null;
		try {  
			ResultSetMetaData metaData = rs.getMetaData();
			int columncount = metaData.getColumnCount(); 
			while (rs.next()) {
				PipeDataUnit u = PipeDataUnit.getInstance();
				for (int i = 1; i < columncount + 1; i++) {
					String v = rs.getString(i);
					String k = metaData.getColumnLabel(i);
					if(k.equals(this.dataPage.get(GlobalParam.READER_KEY))){
						u.setReaderKeyVal(v);
						dataBoundary = v;
					}
					if(k.equals(this.dataPage.get(GlobalParam.READER_SCAN_KEY))){
						LAST_STAMP = v;
					}
					PipeDataUnit.addFieldValue(k, v, transParam,u);
				}
				this.dataUnit.add(u);
			}
			rs.close();
		} catch (Exception e) {
			this.dataPage.put(GlobalParam.READER_STATUS,false);
			EFException err = new EFException(e);
			err.track("get page data exception");
			throw err;
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