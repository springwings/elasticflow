package org.elasticflow.instruction.sets;

import java.lang.reflect.Method;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.elasticflow.instruction.Context;
import org.elasticflow.instruction.Instruction;
import org.elasticflow.model.computer.SampleSets;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.node.CPU;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.Common;

public class ML extends Instruction {

	private final static Logger log = LoggerFactory.getLogger("ML");

	public static DataPage train(Context context, Object[] args) {
		if (!isValid(3, args)) {
			log.error("train parameter not match!");
			return null;
		}
		try {
			Class<?> clz = Class.forName("org.elasticflow.ml.algorithm." + String.valueOf(args[0]));
			Method m = clz.getMethod("train", Context.class, SampleSets.class, Map.class);
			return (DataPage) m.invoke(null, context, args[1], args[2]);
		} catch (Exception e) {
			log.error("train Exception", e);
		}
		return null;
	}

	/**
	 * @param args parameter order is:String contextId, String types, String
	 *             instance, DataPage pageData
	 * @throws Exception
	 */
	public static DataPage batchCompute(Context context, Object[] args) {
		DataPage res = new DataPage();
		if (!isValid(4, args)) {
			log.error("batch Compute parameter not match!");
			return res;
		}
		String contextId = String.valueOf(args[0]);
		String types = String.valueOf(args[1]);
		String instance = String.valueOf(args[2]);
		DataPage dp = (DataPage) args[3];

		if (dp.size() == 0)
			return res;

		DataSetReader DSReader = new DataSetReader();
		DSReader.init(dp);
		long start = Common.getNow();
		int num = 0;
		if (DSReader.status()) {
			try {
				SampleSets samples = SampleSets.getInstance(dp.getData().size());
				while (DSReader.nextLine()) {
					samples.addPoint(DSReader.getLineData(), context.getInstanceConfig().getComputeParams());
					num++;
				}
				res = (DataPage) CPU.RUN(contextId, "ML", "train", false,
						context.getInstanceConfig().getComputeParams().getAlgorithm(), samples,
						context.getInstanceConfig().getWriteFields());
				log.info(Common.formatLog("onepage", " -- " + types + " compute onepage ", instance,
						context.getInstanceConfig().getComputeParams().getAlgorithm(), "", num,
						DSReader.getDataBoundary(), DSReader.getScanStamp(), Common.getNow() - start, ""));
			} catch (Exception e) {
				log.error("batch Compute Exception", e);
			} finally {
				DSReader.close();
			}
		}
		return res;
	}

	/**
	 * @param args parameter order is:String contextId, String types, String
	 *             instance, DataPage pageData
	 * @throws Exception
	 */
	public static DataPage flowCompute(Context context, Object[] args) {
		DataPage res = new DataPage();
		if (!isValid(4, args)) {
			log.error("batchCompute parameter not match!");
			return res;
		}
		String contextId = String.valueOf(args[0]);
		String types = String.valueOf(args[1]);
		String instance = String.valueOf(args[2]);
		DataPage dp = (DataPage) args[3];

		if (dp.size() == 0)
			return res;

		DataSetReader DSReader = new DataSetReader();
		DSReader.init(dp);
		long start = Common.getNow();
		int num = 0;
		if (DSReader.status()) {
			try {
				while (DSReader.nextLine()) {
					res = (DataPage) CPU.RUN(contextId, "ML", "train", false,
							context.getInstanceConfig().getComputeParams().getAlgorithm(),
							SampleSets.genericPoint(DSReader.getLineData(),
									context.getInstanceConfig().getComputeParams()),
							context.getInstanceConfig().getWriteFields());
				} 
				log.info(Common.formatLog("onepage", " -- " + types + " compute onepage ", instance,
						context.getInstanceConfig().getComputeParams().getAlgorithm(), "", num,
						DSReader.getDataBoundary(), DSReader.getScanStamp(), Common.getNow() - start, ""));
			} catch (Exception e) {
				log.error("flowCompute Exception", e);
			} finally {
				DSReader.close();
			}
		}
		return res;
	}
}
