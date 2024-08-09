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
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.model.task.TaskCursor;
import org.elasticflow.model.task.TaskModel;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.util.EFException;

/**
 * Mysql database reader mainly consists of two parts: pagination query and detailed content query
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:24
 */

@ThreadSafe
public class MysqlReader extends ReaderFlowSocket{    

	public static MysqlReader getInstance(final ConnectParams connectParams) {
		MysqlReader o = new MysqlReader();
		o.initConn(connectParams);
		return o;
	}  
 
	@Override
	public DataPage getPageData(final TaskCursor taskCursor,int pageSize) throws EFException {  
		boolean clearConn = false;
		PREPARE(false,false); 
		if(!connStatus())
			return this.dataPage; 
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.reader); 
		this.dataPage.put(GlobalParam.READER_KEY, taskCursor.getReaderKey());
		this.dataPage.put(GlobalParam.READER_SCAN_KEY, taskCursor.getReaderScanKey());
		try (PreparedStatement statement = conn.prepareStatement(taskCursor.getAdditional());){ 
			statement.setFetchSize(pageSize);  
			try(ResultSet rs = statement.executeQuery();){		 
				if(this.readHandler!=null && this.readHandler.supportHandleData()){
					//handler reference getAllData function 
					this.readHandler.handleData(this,rs,taskCursor,pageSize);					
				}else{
					getAllData(rs,taskCursor.getInstanceConfig().getReadFields()); 
				} 
			} catch (Exception e) {
				this.dataPage.put(GlobalParam.READER_STATUS,false); 
				throw new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " mysql get dataPage ResultSet exception");
			} 
		} catch (SQLException e){ 
			this.dataPage.put(GlobalParam.READER_STATUS,false); 
			throw new EFException(e,taskCursor.getAdditional());
		} catch (Exception e) { 
			clearConn = true;
			this.dataPage.put(GlobalParam.READER_STATUS,false); 
			throw new EFException(e,taskCursor.getInstanceConfig().getInstanceID()+ " mysql get dataPage exception");
		}finally{
			releaseConn(false,clearConn);
		} 
		return this.dataPage;
	} 
	
	@Override
	public ConcurrentLinkedDeque<String> getDataPages(final TaskModel task,int pageSize) throws EFException {
		String sql;
		if(task.getScanParam().getPageScanDSL()!=null){
			sql = " select "+GlobalParam._page_field+" as id,(@a:=@a+1) AS EF_ROW_ID from ("
					+ task.getScanParam().getPageScanDSL()
					+ ") EF_FPG_MAIN join (SELECT @a := -1) EF_FPG_ROW order by "+GlobalParam._page_field+" desc";
		}else{
			sql = " select "+GlobalParam._page_field+" as id,(@a:=@a+1) AS EF_ROW_ID from ("
					+ task.getScanParam().getDataScanDSL()
					+ ") EF_FPG_MAIN join (SELECT @a := -1) EF_FPG_ROW order by "+GlobalParam._page_field+" desc"; 
		}
		sql = " select id from (" + sql
				+ ") EF_FPG_END where MOD(EF_ROW_ID, "+pageSize+") = 0";
		sql = sql 
				.replace(GlobalParam._scan_field, task.getScanParam().getScanField())
				.replace(GlobalParam._page_field, task.getScanParam().getPageField())
				.replace(GlobalParam._start_time, task.getStartTime())
				.replace(GlobalParam._end_time, task.getEndTime());
		if (task.getL2seq() != null && task.getL2seq().length() > 0)
			sql = sql.replace(GlobalParam._seq, task.getL2seq()); 
		 
		ConcurrentLinkedDeque<String> page = new ConcurrentLinkedDeque<>();
		PREPARE(false,false); 
		if(!connStatus())
			return page;
		Connection conn = (Connection) GETSOCKET().getConnection(END_TYPE.reader); 
		PreparedStatement statement = null;
		ResultSet rs  = null;
		boolean clearConn = false;
		try {
			boolean autoSelect = true; 
			if(task.getScanParam().getKeyFieldType() != null){
				autoSelect = false;
				if(task.getScanParam().getKeyFieldType().equals("int")){
					statement = conn.prepareStatement(sql.replace(GlobalParam._end, String.valueOf(Long.MAX_VALUE)).replace(
							GlobalParam._start, "0"));
				}else{
					statement = conn.prepareStatement(sql.replace(GlobalParam._end, "~").replace(GlobalParam._start, "0")); 
				}
			}else{
				statement = conn.prepareStatement(sql.replace(GlobalParam._end, String.valueOf(Long.MAX_VALUE)).replace(
						GlobalParam._start,""));
			} 
			statement.setFetchSize(pageSize);
			rs = statement.executeQuery(); 
			while (rs.next()) { 
				page.push(rs.getString("id"));
			}
			if (autoSelect && page.size() == 0) {
				statement.close();
				rs.close();
				statement = conn.prepareStatement(sql.replace(GlobalParam._end, "~")); 
				rs = statement.executeQuery();  
				while (rs.next()) {
					page.push(rs.getString("id"));
				}
			} 
		}catch(SQLException e){
			page = null; 
			throw new EFException(e,sql);
		}catch (Exception e) {
			clearConn = true;
			page = null; 
			throw new EFException(e,task.getInstanceID()+ " mysql reader get page lists exception");
		}finally{ 
			try {
				if(statement!=null && rs!=null) {
					statement.close();
					rs.close();
				}
			} catch (Exception e) {
				clearConn = true;   
				throw new EFException(e,task.getInstanceID()+ " mysql close connection exception");
			} 
			releaseConn(false,clearConn);  
		}  
		return page;
	} 
	
	private void getAllData(ResultSet rs,Map<String, EFField> transParam) throws EFException {   
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
			throw new EFException(e,"mysql get page data exception");
		}
		if (LAST_STAMP==null){ 
			if(this.dataUnit.size()>0)
				this.dataPage.put(GlobalParam.READER_LAST_STAMP, System.currentTimeMillis()); 
		}else{
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, LAST_STAMP); 
		}
		this.dataPage.putDataBoundary(dataBoundary);
		this.dataPage.putData(this.dataUnit);
	} 
}