/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.computer.ComputerFlowSocketFactory;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.ReaderFlowSocketFactory;
import org.elasticflow.searcher.Searcher;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.SearcherSocketFactory;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;
import org.elasticflow.util.instance.TaskUtil;
import org.elasticflow.writer.WriterFlowSocket;
import org.elasticflow.writer.WriterSocketFactory;
import org.elasticflow.yarn.Resource;

/**
 * data-flow router reader searcher computer and writer control center L1seq
 * only support for reader to read series data source and create one or more
 * instance in writer searcherMap and computerMap for data to user data transfer
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-31 13:55
 * @modify 2019-01-10 09:45
 */
public final class SocketCenter {

	/** for special data transfer **/
	private Map<String, Searcher> searcherMap = new ConcurrentHashMap<>();
	/** for normal transfer **/
	private Map<String, PipePump> pipePumpMap = new ConcurrentHashMap<>();
	private Map<String, WriterFlowSocket> writerSocketMap = new ConcurrentHashMap<>();
	private Map<String, ComputerFlowSocket> computerSocketMap = new ConcurrentHashMap<>();
	private Map<String, ReaderFlowSocket> readerSocketMap = new ConcurrentHashMap<>();
	private Map<String, SearcherFlowSocket> searcherSocketMap = new ConcurrentHashMap<>();

	public String getContextId(String instance, String L1seq,String tag) {
		return TaskUtil.getResourceTag(instance, L1seq, tag, false);
	} 
	
	public boolean containsKey(String instance, String L1seq,String tag) {
		return pipePumpMap.containsKey(TaskUtil.getResourceTag(instance, L1seq, tag, false));
	}

	/**
	 * 
	 * build read to write end pipe socket
	 * 
	 * @param L1seq     for series data source sequence
	 * @param instance  data source main tag name
	 * @param needReset for reset resource
	 * @param tag       Marking resource
	 * @throws EFException 
	 */
	public synchronized PipePump getPipePump(String instance, String L1seq, boolean needReset, String tag) throws EFException {
		String tags = TaskUtil.getResourceTag(instance, L1seq, tag, false);
		if (!pipePumpMap.containsKey(tags) || needReset) {
			List<WriterFlowSocket> wfs = new ArrayList<>();
			// Balanced write to multiple targets
			String[] writeDests = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
					.getWriteTo().split(",");
			if (writeDests.length < 1)
				Common.systemLog("instance {} build write pipe socket error,misconfiguration writer destination!",instance);
			for (String dest : writeDests) {
				wfs.add(getWriterSocket(dest, instance, L1seq, tag));
			}
			PipePump pipePump = PipePump
					.getInstance(tags,instance,
							getReaderSocket(Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
									.getReadFrom(), instance, L1seq, tag),
							(Resource.nodeConfig.getInstanceConfigs().get(instance).openCompute()
									? getComputerSocket(instance, L1seq, tag, needReset)
									: null),
							wfs, Resource.nodeConfig.getInstanceConfigs().get(instance),L1seq);
			pipePumpMap.put(tags, pipePump);
		}
		return pipePumpMap.get(tags);
	}

	public synchronized Searcher getSearcher(String instance, String L1seq, String tag, boolean reload) {
		if (reload || !searcherMap.containsKey(instance)) {
			if (!Resource.nodeConfig.getSearchConfigs().containsKey(instance)) {
				Common.systemLog("instance {} get searcher exception,instance not exist",instance);
				return null;
			}
			InstanceConfig instanceConfig = Resource.nodeConfig.getSearchConfigs().get(instance);
			Searcher searcher = Searcher.getInstance(instance, instanceConfig,
					getSearcherSocket(
							Resource.nodeConfig.getSearchConfigs().get(instance).getPipeParams().getSearchFrom(),
							instance, L1seq, tag, reload));
			searcherMap.put(instance, searcher);
		}
		return searcherMap.get(instance);
	}

	/**
	 * Dismantling pipelines
	 * @param instance
	 * @param L1seq
	 * @param tag
	 */
	public synchronized void clearPipePump(String instance, String L1seq, String tag) {
		String resourceTag = TaskUtil.getResourceTag(instance, L1seq, tag, false);
		if (pipePumpMap.containsKey(resourceTag)) { 
			pipePumpMap.remove(resourceTag);
			boolean ignoreSeqUseAlias = false;
			if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isReaderPoolShareAlias(); //Determine whether to share resources
			//Branch determined by the reader
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			resourceTag = TaskUtil.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);
			readerSocketMap.get(resourceTag).release();
			readerSocketMap.remove(resourceTag); 
			writerSocketMap.get(resourceTag).release();
			writerSocketMap.remove(resourceTag);
		}
	}

	public synchronized ReaderFlowSocket getReaderSocket(String resourceName, String instance, String L1seq, String tag) throws EFException {		
		boolean ignoreSeqUseAlias = false;
		if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
			ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
					.isReaderPoolShareAlias();//Determine whether to share resources
		String tagInstance = instance;
		if (ignoreSeqUseAlias)
			tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
		String tags = TaskUtil.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);

		if (!readerSocketMap.containsKey(tags)) {
			WarehouseParam whp = getWHP(resourceName);
			if (whp == null) {
				Common.systemLog("instance {} get reader socket exception,resource {} not exist!",instance,resourceName);
				Common.stopSystem(false);
			}
			readerSocketMap.put(tags, ReaderFlowSocketFactory.getInstance(
					ConnectParams.getInstance(whp, L1seq, Resource.nodeConfig.getInstanceConfigs().get(instance),
							null),
					L1seq,
					Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getCustomReader()));
			readerSocketMap.get(tags).prepareFlow(Resource.nodeConfig.getInstanceConfigs().get(instance),END_TYPE.reader,L1seq);
		}
		return readerSocketMap.get(tags);
	}

	public synchronized ComputerFlowSocket getComputerSocket(String instance, String L1seq, String tag, boolean reload) throws EFException {
		String tags = TaskUtil.getResourceTag(instance, L1seq, tag, false);
		if (reload || !computerSocketMap.containsKey(tags)) {
			computerSocketMap.put(tags, ComputerFlowSocketFactory.getInstance(ConnectParams.getInstance(null,
					null, Resource.nodeConfig.getInstanceConfigs().get(instance), null)));
			computerSocketMap.get(tags).prepareFlow(Resource.nodeConfig.getInstanceConfigs().get(instance),END_TYPE.computer,L1seq);
		}
		return computerSocketMap.get(tags);
	}

	public synchronized WriterFlowSocket getWriterSocket(String resourceName, String instance, String L1seq, String tag) throws EFException {		
		String tags = TaskUtil.getResourceTag(instance, L1seq, tag, false);
		if (!writerSocketMap.containsKey(tags)) {
			WarehouseParam whp = getWHP(resourceName);
			if (whp == null) {
				Common.systemLog("instance {} get writer socket exception,resource {} not exist!",instance,resourceName);
				Common.stopSystem(false);
			}
			writerSocketMap.put(tags, WriterSocketFactory.getInstance(
					ConnectParams.getInstance(whp, L1seq, Resource.nodeConfig.getInstanceConfigs().get(instance),
							null),
					L1seq,
					Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getCustomWriter()));
			writerSocketMap.get(tags).prepareFlow(Resource.nodeConfig.getInstanceConfigs().get(instance),END_TYPE.writer,L1seq);
		}
		return writerSocketMap.get(tags);
	}

	public synchronized SearcherFlowSocket getSearcherSocket(String resourceName, String instance, String L1seq, String tag,
			boolean reload) {		
		boolean ignoreSeqUseAlias = false;
		if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
			ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
					.isSearcherShareAlias();//Determine whether to share resources
		String tagInstance = instance;
		if (ignoreSeqUseAlias)
			tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
		String tags = TaskUtil.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);

		if (reload || !searcherSocketMap.containsKey(tags)) {
			WarehouseParam whp = getWHP(resourceName);
			if (whp == null) {
				Common.systemLog("instance {} get searcher socket exception,resource {} not exist!",instance,resourceName);
				Common.stopSystem(false);
			} 
			SearcherFlowSocket searcher = SearcherSocketFactory
					.getInstance(
							ConnectParams.getInstance(whp, L1seq,
									Resource.nodeConfig.getInstanceConfigs().get(instance), null),
							Resource.nodeConfig.getSearchConfigs().get(instance), null);
			if(searcher!=null)
				searcherSocketMap.put(tags, searcher);
		}
		return searcherSocketMap.get(tags);
	}

	public WarehouseParam getWHP(String destination) {
		WarehouseParam param = null;
		if (Resource.nodeConfig.getWarehouse().containsKey(destination)) {
			param = Resource.nodeConfig.getWarehouse().get(destination);
		}
		return param;
	}
}
