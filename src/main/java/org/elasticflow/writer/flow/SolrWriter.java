package org.elasticflow.writer.flow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.field.EFField;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.end.WriterParam;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;
import org.elasticflow.writer.WriterFlowSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * solr flow Writer Manager
 * @author chengwen
 * @version 2.0
 * @date 2018-10-30 14:08
 */
@NotThreadSafe
public class SolrWriter extends WriterFlowSocket{
	 
	private static String linux_spilt= "/";
	private static String srcDir= "config/template";
	private static String zkDir = "/configs";
	private final static int BUFFER_LEN = 1024;
	private final static int END = -1; 
	protected CloudSolrClient CONNS;
	private List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(); 
	
	private final static Logger log = LoggerFactory.getLogger("SolrFlow");  
	
	public static SolrWriter getInstance(ConnectParams connectParams) {
		SolrWriter o = new SolrWriter();
		o.INIT(connectParams);
		return o;
	}
	
	@Override
	public void INIT(ConnectParams connectParams) {
		this.connectParams = connectParams; 
		this.poolName = connectParams.getWhp().getPoolName(connectParams.getL1Seq());
		this.isBatch = GlobalParam.WRITE_BATCH;
	}
 
	 
	@Override
	public boolean create(String instantcName, String storeId, InstanceConfig instanceConfig) {
		String name = Common.getStoreName(instantcName, storeId); 
		try {
			log.info("create index " + name); 
			String zkHost = getSolrConn().getZkHost();
			moveFile2ZookeeperDest((GlobalParam.configPath+"/"+srcDir).replace("file:", ""), zkDir+"/"+instantcName, zkHost);
			getSchemaFile(instanceConfig.getWriteFields(), instantcName, storeId, zkHost); 
			CollectionAdminRequest.Create create = new CollectionAdminRequest.Create();
			create.setConfigName(instantcName);
			create.setCollectionName(name);
			create.setNumShards(3);
			create.setMaxShardsPerNode(2);
			create.setReplicationFactor(2);
			create.process(getSolrConn(), name);
			return true;
		} catch (Exception e) {
			log.error("setting core " + name +" failed.",e); 
		} 
		return false;
	} 
	
	@Override
	public void write(WriterParam writerParam,PipeDataUnit unit,Map<String, EFField> writeParamMap, String instantcName, String storeId,boolean isUpdate) throws EFException { 
		String name = Common.getStoreName(instantcName,storeId);
		if (unit.getData().size() == 0){
			log.warn("Empty IndexUnit for " + name );
			return;
		} 
		if(getSolrConn().getDefaultCollection() == null){
			getSolrConn().setDefaultCollection(name);
		}  
		SolrInputDocument doc = new SolrInputDocument(); 
		for(Entry<String, Object> r:unit.getData().entrySet()){
			String field = r.getKey(); 
			if (r.getValue() == null)
				continue;
			String value = String.valueOf(r.getValue());
			EFField transParam = writeParamMap.get(field);
			if (transParam == null)
				continue;
			
			if(isUpdate){  
				if (transParam.getAnalyzer().length()==0){
					if (transParam.getSeparator() != null){
						String[] vs = value.split(transParam.getSeparator());
						Map<String,Object> fm = new HashMap<>(1);
						fm.put("set",vs);
						doc.setField(transParam.getAlias(), fm);
					}
					else{
						if(writerParam.getWriteKey().equals(field)){
							doc.setField(transParam.getAlias(), value);
						}else{
							Map<String, String> fm = new HashMap<>(1);
							fm.put("set",value);
							doc.setField(transParam.getAlias(), fm);
						}  
					} 
				} else {
					Map<String, String> fm = new HashMap<>(1);
					fm.put("set",value);
					doc.setField(transParam.getAlias(), fm);
				}
			}else{
				if (transParam.getAnalyzer().length()==0){
					if (transParam.getSeparator() != null){
						String[] vs = value.split(transParam.getSeparator());
						doc.addField(transParam.getAlias(), vs);
					}
					else
						doc.addField(transParam.getAlias(), value); 
				} else {
					doc.addField(transParam.getAlias(), value);
				}
			} 
		}  
		if (!this.isBatch) {
			try {
				getSolrConn().add(doc);
				getSolrConn().commit();
			}catch (Exception e) {
				throw new EFException(e);
			} 
		}else{
			synchronized (docs) {
				docs.add(doc); 
			}
		}
	}
	 
	@Override
	public void delete(String instance, String storeId,String keyColumn, String keyVal) throws EFException {  
	 
	} 

	@Override
	public void removeInstance(String instanceName, String storeId) {
		if(null == storeId){
			log.info("storeId is Null");
			return;
		} 
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to remove core " + name);
			CollectionAdminRequest.Delete delete = new CollectionAdminRequest.Delete();
			delete.setCollectionName(name);
			delete.process(getSolrConn(),name);
			log.info("solr core " + name + " removed Success!"); 
		} catch (Exception e) {
			log.error("remove core " + name + " Exception,",e);
		} 
	}

	@Override
	public void setAlias(String instanceName, String storeId, String aliasName) { 
		String name = Common.getStoreName(instanceName, storeId);
		try {
			log.info("trying to setting Alias " + aliasName + " to collection " + name);		
			CollectionAdminRequest.CreateAlias createAlias = new CollectionAdminRequest.CreateAlias(); 
			CollectionAdminResponse response = createAlias.setAliasName(instanceName).setAliasedCollections(name).process(getSolrConn());
			if (response.isSuccess()){
				log.info("alias " + aliasName + " setted to " + name);
			}
		} catch (Exception e) {
			log.error("alias " + aliasName + " set to " + name + " Exception,",e); 
		}  
	} 

	@Override
	public void flush() throws EFException { 
		if(this.isBatch){
			synchronized (docs){
				try { 
					getSolrConn().add(docs);
					getSolrConn().commit(true, true, true);
				} catch (Exception e) {
					if (Common.exceptionCheckContain(e, "Collection not found")) {
						throw new EFException("storeId not found",ELEVEL.Dispose,ETYPE.WRITE_POS_NOT_FOUND);
					} else {
						throw new EFException(e);
					}
				}
				docs.clear();
			} 
		} 
	}

	@Override
	public void optimize(String instanceName, String storeId)  { 
		String name = Common.getStoreName(instanceName, storeId);
		try {
			UpdateResponse updateRespons = getSolrConn().optimize(name, true, true, 1);
			int status = updateRespons.getStatus();
			if (status > 0) {
				log.warn("index " + name + " optimize Failed ");
			}
		} catch (Exception e) {
			log.error("index " + name + " optimize failed.",e); 
		} 
	} 

	
	protected String abMechanism(String mainName, boolean isIncrement, InstanceConfig instanceConfig) {
		String b = Common.getStoreName(mainName, "b");
		String a = Common.getStoreName(mainName, "a");
		String select="";  
		if(isIncrement){
			if(this.storePositionExists(a)){ 
				select = "a";
			}else if(this.storePositionExists(b)){
				select = "b"; 
			}else{
				select = "a"; 
				create(mainName,select, instanceConfig);
				setAlias(mainName, select, instanceConfig.getAlias());
			}   
		}else{
			select =  "b";
			if(this.storePositionExists(b)){ 
				select =  "a";
			}
			create(mainName,select, instanceConfig);
		} 
		return select;
	}

	private void getSchemaFile(Map<String,EFField> paramMap,String instantcName, String storeId,String zkHost) {
		BufferedReader head_reader = null;
		BufferedReader tail_reader = null;  
		String destPath = zkDir+"/"+instantcName+"/schema.xml";
		ZooKeeper zk = null;
		Stat stat = null;
		final CountDownLatch connectedSemaphore = new CountDownLatch( 1 ); //防止出现ConnectionLossException
		StringBuilder sb = new StringBuilder();
		try { 
			Watcher watcher = new Watcher() { 
				public void process(WatchedEvent event) {
					connectedSemaphore.countDown();
				}
			}; 
			zk = new ZooKeeper(zkHost, 5000, watcher);
			connectedSemaphore.await();
			 
			stat = zk.exists(destPath, watcher);
			if (null == stat) {
				zk.create(destPath, "".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			 
			String head = GlobalParam.configPath+"/"+srcDir+"/schema_head.txt";
			String tail = GlobalParam.configPath+"/"+srcDir+"/schema_tail.txt";
			head_reader = new BufferedReader(new InputStreamReader(new FileInputStream(head.replace("file:", "")), "UTF-8"));
			tail_reader = new BufferedReader(new FileReader(tail.replace("file:", "")));
			String str = null;
			while ((str = head_reader.readLine()) != null) {
				sb.append(str).append("\n");
			}
			
			StringBuilder field = new StringBuilder();
			String firstFiled = null;
			for (Map.Entry<String, EFField> e : paramMap.entrySet()) {
				EFField tp = e.getValue();
				field.delete(0, field.length());
				if (tp.getName() == null)
					continue;
				if(firstFiled == null){
					firstFiled = tp.getName();
				}
				field.append("<field ").append("name=\"").append(tp.getAlias()).append("\" ");
				if("string".equals(tp.getIndextype()) && tp.getAnalyzer().length()==0) {
					field.append("type=\"").append("string\" ");
				}else{
					field.append("type=\"").append(tp.getIndextype()).append("\" ");
				}
				field.append("indexed=\"").append(tp.getIndexed()).append("\" ");
				field.append("stored=\"").append(tp.getStored()).append("\" ");
				field.append("required=\"false\" />");
				sb.append(field.toString()).append("\n");
			}
			
			String uniqeKey = "<uniqueKey>"+firstFiled+"</uniqueKey>";
			String defaultSearchField = "<defaultSearchField>"+firstFiled+"</defaultSearchField>";
			sb.append(uniqeKey).append("\n");
			sb.append(defaultSearchField).append("\n");
			while ((str = tail_reader.readLine()) != null) {
				sb.append(str).append("\n");
			}

			zk.setData(destPath, sb.toString().getBytes(), -1);
		} catch (Exception e) {
			log.error("getSchemaFile Exception ",e); 
		} finally {
			try {
				if(zk != null){
					zk.close();
				}
				head_reader.close();
				tail_reader.close();
			} catch (Exception e) {
				log.error("zookeeper close Exception" ,e); 
			}

		}
	} 
	
	private static void moveFile(String sourceAdd, ZooKeeper zk,Watcher watcher, String destinationAdd) { 
		InputStream in = null;
		Stat stat = null;
		String remoteAdd = null;
		try {
			File file = new File(sourceAdd); 
			if(!file.isDirectory()){
				in = new FileInputStream(sourceAdd);
				remoteAdd = destinationAdd+linux_spilt+file.getName(); 
				stat = zk.exists(remoteAdd, watcher);
				if (null == stat) {
					zk.create(remoteAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
				StringBuilder sb = new StringBuilder();
				byte[] buffer = new byte[BUFFER_LEN];
				while (true) {
					int byteRead = in.read(buffer);
					if (byteRead == END)
						break;
					sb.append(new String(buffer, 0, byteRead));
				}
				zk.setData(remoteAdd, sb.toString().getBytes(), -1); 
			}else{
                String[] filelist = file.list();
                for (int i = 0; i < filelist.length; i++) {
                    File readfile = new File(sourceAdd + File.separator + filelist[i]);
                    if (!readfile.isDirectory()) {
                   	   in = new FileInputStream(sourceAdd +  File.separator + filelist[i]);
                   	   
                   	   stat = zk.exists(destinationAdd, watcher);
					   if (null == stat) {
						zk.create(destinationAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					    } 
						remoteAdd =  destinationAdd+linux_spilt + file.getName();
						stat = zk.exists(remoteAdd, watcher);
						if (null == stat) {
							zk.create(remoteAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						remoteAdd =  destinationAdd+ linux_spilt + file.getName()+linux_spilt+readfile.getName(); 
						stat = zk.exists(remoteAdd, watcher);
						if (null == stat) {
							zk.create(remoteAdd, "".getBytes(),Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						} 
						StringBuilder sb = new StringBuilder();
						byte[] buffer = new byte[BUFFER_LEN];
						while (true) {
							int byteRead = in.read(buffer);
							if (byteRead == END)
								break;
							sb.append(new String(buffer,0,byteRead));
						}
						zk.setData(remoteAdd, sb.toString().getBytes(), -1); 
                    } else if (readfile.isDirectory()) { 
                     String sourceAdd2 = sourceAdd + File.separator + readfile.getName();
                     String destinationAdd2 = destinationAdd+linux_spilt+file.getName(); 
                   	 moveFile(sourceAdd2,zk,watcher,destinationAdd2);
                    }
                }
			}
		} catch (Exception e) {
			log.error("moveFile error,",e);
		} finally {
			try {
				if(null != in){
					in.close();
				}
			} catch (Exception e) {
				log.error("zookeeper close Exception," ,e);
			}
		}
	}
	
	private static void moveFile2ZookeeperDest( String sourceAdd, String destinationAdd,String zkHost) {
		ZooKeeper zk = null;
		Stat stat = null;
		final CountDownLatch connectedSemaphore = new CountDownLatch( 1 );  
		try { 
			Watcher watcher = new Watcher() { 
				public void process(WatchedEvent event) {
					connectedSemaphore.countDown();
				}
			}; 
			zk = new ZooKeeper(zkHost, 5000, watcher);
			connectedSemaphore.await();
			 
			stat = zk.exists(destinationAdd, watcher);
			if (null == stat) {
				zk.create(destinationAdd, "".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			}
			File file = new File(sourceAdd);
			if (file.isDirectory()) {
				String[] filelist = file.list();
				for(String i_file : filelist){
					moveFile(sourceAdd+File.separator+i_file, zk, watcher, destinationAdd);
				}
			} 
		} catch (Exception e) {
			log.error("moveFile2ZookeeperDest Exception,",e); 
		} finally {
			try {
				if (null != zk) {
					zk.close();
				}
			} catch (Exception e) {
				log.error("zk close Exception,",e); 
			}
		} 
	}
	 
	
	private CloudSolrClient getSolrConn() { 
		synchronized (this) {
			if(this.CONNS==null)
				this.CONNS = (CloudSolrClient) GETSOCKET().getConnection(END_TYPE.writer);
			return this.CONNS;
		} 
	}

	@Override
	public boolean storePositionExists(String storeName) {
		SolrQuery qb = new SolrQuery();
		try {
			getSolrConn().query(storeName, qb);
			return true;
		} catch (Exception e) {
			return false;
		} 
	}
}
