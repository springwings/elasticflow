package org.elasticflow.node;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticflow.computer.Computer;
import org.elasticflow.config.InstanceConfig;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.param.warehouse.WarehouseParam;
import org.elasticflow.piper.PipePump;
import org.elasticflow.reader.ReaderFlowSocket;
import org.elasticflow.reader.ReaderFlowSocketFactory;
import org.elasticflow.searcher.Searcher;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.SearcherSocketFactory;
import org.elasticflow.util.Common;
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
	private Map<String, Computer> computerMap = new ConcurrentHashMap<>();

	/** for normal transfer **/
	private Map<String, PipePump> pipePumpMap = new ConcurrentHashMap<>();
	private Map<String, WriterFlowSocket> writerSocketMap = new ConcurrentHashMap<>();
	private Map<String, ReaderFlowSocket> readerSocketMap = new ConcurrentHashMap<>();
	private Map<String, SearcherFlowSocket> searcherSocketMap = new ConcurrentHashMap<>();

	/**
	 * 
	 * build read to write end pipe socket
	 * @param L1seq        for series data source sequence
	 * @param instance data source main tag name
	 * @param needClear    for reset resource
	 * @param tag          Marking resource
	 */
	public PipePump getPipePump(String instance, String L1seq, boolean needClear, String tag) {
		synchronized (pipePumpMap) {
			String tags = Common.getResourceTag(instance, L1seq, tag, false);
			if (!pipePumpMap.containsKey(tags) || needClear) {
				PipePump transDataFlow = PipePump.getInstance(
						getReaderSocket(
								Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadFrom(),
								instance, L1seq, tag),
						(Resource.nodeConfig.getInstanceConfigs().get(instance).getComputeParams().getComputeModel()
								.equals("flow")
										? getReaderSocket(Resource.nodeConfig.getInstanceConfigs().get(instance)
												.getPipeParams().getWriteTo(), instance, L1seq, tag)
										: null),
						getWriterSocket(
								Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(),
								instance, L1seq, tag),
						Resource.nodeConfig.getInstanceConfigs().get(instance));
				pipePumpMap.put(tags, transDataFlow);
			}
			return pipePumpMap.get(tags);
		}
	}

	public Computer getComputer(String instance, String L1seq, String tag, boolean reload) {
		synchronized (computerMap) {
			if (reload || !computerMap.containsKey(instance)) {
				if (!Resource.nodeConfig.getInstanceConfigs().containsKey(instance))
					return null;
				InstanceConfig instanceConfig = Resource.nodeConfig.getInstanceConfigs().get(instance);
				Computer computer = Computer.getInstance(instance, instanceConfig,
						getReaderSocket(
								Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getModelFrom(),
								instance, L1seq, tag));
				computerMap.put(instance, computer);
			}
		}
		return computerMap.get(instance);
	}

	public Searcher getSearcher(String instance, String L1seq, String tag, boolean reload) {
		synchronized (searcherMap) {
			if (reload || !searcherMap.containsKey(instance)) {
				if (!Resource.nodeConfig.getSearchConfigs().containsKey(instance))
					return null;
				InstanceConfig instanceConfig = Resource.nodeConfig.getSearchConfigs().get(instance);
				Searcher searcher = Searcher.getInstance(instance, instanceConfig,
						getSearcherSocket(
								Resource.nodeConfig.getSearchConfigs().get(instance).getPipeParams().getSearchFrom(),
								instance, L1seq, tag, reload));
				searcherMap.put(instance, searcher);
			}
			return searcherMap.get(instance);
		}
	}

	public void clearPipePump(String instance, String L1seq, String tag) {
		synchronized (this) {
			String tags = Common.getResourceTag(instance, L1seq, tag, false);
			if (pipePumpMap.containsKey(tags)) {
				pipePumpMap.remove(tags);

				boolean ignoreSeqUseAlias = false;
				if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
					ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
							.isReaderPoolShareAlias();
				String tagInstance = instance;
				if (ignoreSeqUseAlias)
					tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
				tags = Common.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);
				readerSocketMap.remove(tags);
				writerSocketMap.remove(tags);
			}
		}
	}

	public ReaderFlowSocket getReaderSocket(String resourceName, String instance, String L1seq, String tag) {
		synchronized (readerSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isReaderPoolShareAlias();
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);

			if (!readerSocketMap.containsKey(tags)) {
				WarehouseParam whp = getWHP(resourceName);
				if (whp == null)
					return null;
				readerSocketMap.put(tags, ReaderFlowSocketFactory.getInstance(
						ConnectParams.getInstance(whp, L1seq, Resource.nodeConfig.getInstanceConfigs().get(instance),
								null),
						L1seq,
						Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadHandler()));
			}
			return readerSocketMap.get(tags);
		}
	}

	public WriterFlowSocket getWriterSocket(String resourceName, String instance, String L1seq, String tag) {
		synchronized (writerSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isWriterPoolShareAlias();
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);

			if (!writerSocketMap.containsKey(tags)) {
				WarehouseParam whp = getWHP(resourceName);
				if (whp == null)
					return null;
				writerSocketMap.put(tags, WriterSocketFactory.getInstance(
						ConnectParams.getInstance(whp, L1seq, Resource.nodeConfig.getInstanceConfigs().get(instance),
								null),
						L1seq,
						Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteHandler()));
			}
			return writerSocketMap.get(tags);
		}
	}

	public SearcherFlowSocket getSearcherSocket(String resourceName, String instance, String L1seq, String tag,
			boolean reload) {
		synchronized (searcherSocketMap) {
			boolean ignoreSeqUseAlias = false;
			if (Resource.nodeConfig.getInstanceConfigs().get(instance) != null)
				ignoreSeqUseAlias = Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams()
						.isSearcherShareAlias();
			String tagInstance = instance;
			if (ignoreSeqUseAlias)
				tagInstance = Resource.nodeConfig.getInstanceConfigs().get(instance).getAlias();
			String tags = Common.getResourceTag(tagInstance, L1seq, tag, ignoreSeqUseAlias);

			if (reload || !searcherSocketMap.containsKey(tags)) {
				WarehouseParam whp = getWHP(resourceName);
				if (whp == null)
					return null;
				SearcherFlowSocket searcher = SearcherSocketFactory
						.getInstance(
								ConnectParams.getInstance(whp, L1seq,
										Resource.nodeConfig.getInstanceConfigs().get(instance), null),
								Resource.nodeConfig.getSearchConfigs().get(instance), null);
				searcherSocketMap.put(tags, searcher);
			}
			return searcherSocketMap.get(tags);
		}
	}

	public WarehouseParam getWHP(String destination) {
		WarehouseParam param = null;
		if (Resource.nodeConfig.getNoSqlWarehouse().containsKey(destination)) {
			param = Resource.nodeConfig.getNoSqlWarehouse().get(destination);
		} else if (Resource.nodeConfig.getSqlWarehouse().containsKey(destination)) {
			param = Resource.nodeConfig.getSqlWarehouse().get(destination);
		}
		return param;
	}
}
