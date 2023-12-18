package org.elasticflow.util.instance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;

import org.elasticflow.util.Common;

/**
 * Instance related configuration, running resources and running data utils
 * 
 * @author chengwen
 * @version 1.2
 * @date 2018-10-11 11:00
 */
public class EFDataStorer {

	public static boolean exists(String filePath) {
		try {
			File file = new File(filePath);
			return file.exists();
		} catch (Exception e) {
			Common.LOG.error("file {} exists check exception",filePath, e);
			return false;
		}
	}

	public static void createPath(String filePath,boolean isFile) {
		try {
			File fd = new File(filePath);
			if(isFile) {
				fd.createNewFile();
			}else {
				fd.mkdirs();
			}
		} catch (Exception e) {
			Common.LOG.error("create path {} exception",filePath, e);
		}
	}

	public static void setData(String filePath, String data) {
		try {
			try (FileWriter fw = new FileWriter(filePath);) {
				fw.write(data);
			} catch (Exception e) {
				throw e;
			}
		} catch (Exception e) {
			Common.LOG.error("write data to {} exception",filePath,e);
		}
	}
	
	public static byte[] getData(String filePath,boolean create) {
		try (FileInputStream fi = new FileInputStream(filePath);) {
			File f = new File(filePath);
            int length = (int) f.length();
            byte[] data = new byte[length];
            fi.read(data);
	        return data;
		}catch (FileNotFoundException e1) {
			try {
				if(create) { 
					File f = new File(filePath);
					File fileParent = f.getParentFile();
					if(!fileParent.exists()){
						fileParent.mkdirs();
					}
					f.createNewFile();
				}
				return "".getBytes();
			}catch(Exception e2) {
				Common.LOG.error("create file {} exception",filePath, e2);
				return null;
			}
		} catch (Exception e) {
			Common.LOG.error("read data from {} exception",filePath, e);
			return null;
		}
	}
}
