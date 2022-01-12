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
	
	public static double getCpuUsage() throws Exception {
		double cpuUsed = 0;
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
			throw e;
		}
		return cpuUsed;
	}

	public static double getMemUsage() throws Exception {
		double memUsed = 0;
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
						if (++m == 4) {
							memUsed = Double.parseDouble(tmp);
						}
					}
				}
			}
		} catch (Exception e) {
			throw e;
		}
		return memUsed;
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
