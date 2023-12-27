package org.elasticflow.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.elasticflow.config.GlobalParam;

/**
 * get system info
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:14
 */
public class SystemInfoUtil {
	
	public static double getCpuUsage() {
		double cpuUsed = 0;
		try {
			Runtime rt = Runtime.getRuntime(); 
			Process p = rt.exec("ps aux");
			try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
				String str = null;
				String[] strArray = null;
				while ((str = in.readLine()) != null) {
					int m = 0;
					if (str.contains(GlobalParam.PROJ.toLowerCase())) {
						strArray = str.split(" ");
						for (String tmp : strArray) {
							if (tmp.trim().length() == 0)
								continue;
							if (++m == 3) {
								cpuUsed = Double.parseDouble(tmp);
							}
						}
					}
				}
			} catch (Exception e) {
				Common.LOG.warn("get cpu usage exception",e);
			}
		} catch (Exception e) {
			Common.LOG.warn("get cpu usage exception",e);
		} 
		return cpuUsed;
	}
	
	/**
	 * Memory usage ratio (JVM)
	 * @return
	 * @throws Exception
	 */
	public static double getMemUsage() { 
		try {
			Runtime runtime = Runtime.getRuntime();  
			double totalMemoryBytes = runtime.totalMemory();  
			double usedMemoryBytes = totalMemoryBytes - runtime.freeMemory();  
			return Math.round(usedMemoryBytes/totalMemoryBytes * 100.0) / 1.0;
		}catch(Exception e) {
			Common.LOG.warn("get memory exception,",e);
		} 
		return 0.0;
	}
	
	public static double getMemTotal() { 
		try {
			Runtime runtime = Runtime.getRuntime();  
			long totalMemoryBytes = runtime.totalMemory();    
			return  Math.round(totalMemoryBytes / Math.pow(1024, 3) * 10.0) / 10.0;
		}catch(Exception e) {
			Common.LOG.warn("get memory exception,",e);
		} 
		return 0.0;
	}

	/***
	 * 获取线程数
	 *  
	 */
	public static String getThreads(String name) throws Exception {
		int nums = 0;
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec("top -b -n 1");
		try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
			String str = null;
			while ((str = in.readLine()) != null) {
				if (str.contains(name)) {
					nums++;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return String.valueOf(nums);  
	}

}
