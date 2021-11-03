package org.elasticflow.util.instance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;

import org.elasticflow.config.GlobalParam;
import org.elasticflow.util.Common;

/**
 * Instance related configuration, running resources and running data utils
 * 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */
public class EFDataStorer {

	public static boolean exists(String path) {
		try {
			if (GlobalParam.USE_ZK) {
				return ZKUtil.getZk().exists(path, false) == null ? false : true;
			} else {
				File file = new File(path);
				return file.exists();
			}
		} catch (Exception e) {
			return false;
		}
	}

	public static void createPath(String path,boolean isFile) {
		try {
			if (GlobalParam.USE_ZK) {
				ZKUtil.createPath(path, true);
			} else {
				File fd = new File(path);
				if(isFile) {
					fd.createNewFile();
				}else {
					fd.mkdirs();
				}
			}
		} catch (Exception e) {
			Common.LOG.error("environmentCheck Exception", e);
		}
	}

	public static void setData(String path, String data) {
		try {
			if (GlobalParam.USE_ZK) {
				ZKUtil.setData(path,data);
			} else {
				try (FileWriter fw = new FileWriter(path);) {
					fw.write(data);
				} catch (Exception e) {
					throw e;
				}
			}
		} catch (Exception e) {
			Common.LOG.error("write data Exception", e);
		}
	}
	
	public static byte[] getData(String path,boolean create) {
		if (GlobalParam.USE_ZK) {
			return ZKUtil.getData(path,create);
		} else {
			try (FileInputStream fi = new FileInputStream(path);) {
				File f = new File(path);
	            int length = (int) f.length();
	            byte[] data = new byte[length];
	            fi.read(data);
		        return data;
			}catch (FileNotFoundException e1) {
				try {
					if(create) { 
						File f = new File(path);
						f.createNewFile();
					}
					return "".getBytes();
				}catch(Exception e2) {
					Common.LOG.error("create file Exception", e2);
					return null;
				}
			} catch (Exception e) {
				Common.LOG.error("read data Exception", e);
				return null;
			}
		}
	}

}
