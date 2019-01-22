package org.elasticflow.instruction.sets;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.node.CPU;
import org.elasticflow.yarn.Resource;

/**
 *  * runtime manage
 * @author chengwen
 * @version 2.1
 * @date 2018-11-02 16:47
 */
public class Track extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("Track");

	static HashMap<String, HashMap<String, Object>> tmpStore = new HashMap<>();

	public static boolean cpuPrepare(Context context, Object[] args) {
		if (context != null)
			return true;
		String L1seq = null;
		String instance;
		String id;
		if (args.length == 2) {
			instance = (String) args[0];
			id = (String) args[1];
		} else if (args.length == 3) {
			instance = (String) args[0];
			L1seq = (String) args[1];
			id = (String) args[2];
		} else {
			return false;
		}
		CPU.prepare(id, Resource.nodeConfig.getInstanceConfigs().get(instance),
				Resource.SOCKET_CENTER.getWriterSocket(
						Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getWriteTo(), instance,
						L1seq, ""),
				Resource.SOCKET_CENTER.getReaderSocket(
						Resource.nodeConfig.getInstanceConfigs().get(instance).getPipeParams().getReadFrom(), instance,
						L1seq, ""),
				(Resource.nodeConfig.getInstanceConfigs().get(instance).getComputeParams().getComputeModel()
						.equals("flow")
								? Resource.SOCKET_CENTER.getReaderSocket(Resource.nodeConfig.getInstanceConfigs().get(instance)
										.getPipeParams().getWriteTo(), instance, L1seq, "")
								: null));
		return true;

	}

	public static boolean cpuFree(Context context, Object[] args) {
		if (isValid(1, args)) {
			String id = (String) args[0];
			if (tmpStore.containsKey(id)) {
				tmpStore.remove(id);
			}
		}
		return true;
	}

	/**
	 * @param args
	 *            parameter order is: String key,Object val
	 */
	public static void store(Context context, Object[] args) {
		if (isValid(3, args)) {
			String key = (String) args[0];
			Object val = args[1];
			String id = (String) args[2];
			if (!tmpStore.containsKey(id)) {
				tmpStore.put(id, new HashMap<String, Object>());
			}
			tmpStore.get(id).put(key, val);
		} else {
			log.error("store parameter not match!");
		}
	}

	/**
	 * @param args
	 *            parameter order is: String key
	 */
	public static Object fetch(Context context, Object[] args) {
		if (isValid(2, args)) {
			String key = (String) args[0];
			String id = (String) args[1];
			if (tmpStore.containsKey(id)) {
				return tmpStore.get(id).get(key);
			}
		} else {
			log.error("fetch parameter not match!");
		}
		return null;
	}
}
