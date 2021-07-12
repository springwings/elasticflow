package org.elasticflow.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * get system info
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:14
 */
public class SystemInfoUtil {
	public static String getCpuUsage() throws Exception {
		double cpuUsed = 0;
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec("top -b -n 1");
		try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
			String str = null;
			String[] strArray = null;
			while ((str = in.readLine()) != null) {
				int m = 0;
				if (str.contains("java")) {
					strArray = str.split(" ");
					for (String tmp : strArray) {
						if (tmp.trim().length() == 0)
							continue;
						if (++m == 9) {
							cpuUsed += Double.parseDouble(tmp);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return String.valueOf(cpuUsed);
	}

	public static String getMemUsage() throws Exception {
		int menUsed = 0;
		int menTotal = 0;
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec("free -m");
		try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
			String str = null;
			String[] strArray = null;
			while ((str = in.readLine()) != null) {
				int m = 0;
				if (str.contains("Mem")) {
					strArray = str.split(" ");
					for (String tmp : strArray) {
						if(tmp.length()<1){
							continue;
						}
						m++;
						if (m == 2) {
							menTotal = Integer.parseInt(tmp);
						}
						if (m == 3) {
							menUsed = Integer.parseInt(tmp);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return String.valueOf(menUsed*100/(menTotal+0.0d));  
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
