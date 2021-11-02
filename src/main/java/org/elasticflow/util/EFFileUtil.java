package org.elasticflow.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class EFFileUtil { 

	public static void renameFile(String filePath, String oldName, String newName){
		if(!oldName.equals(newName)){
			File oldFile = new File(filePath+"/"+oldName);
			File newFile = new File(filePath+"/"+newName);
			if(newFile.exists()){
				Common.LOG.warn(newName+" the file already exists！"); 
			} else {
				oldFile.renameTo(newFile);
			}
		}
	}
	
	public static boolean delFile(String filePath) {
		try{
            File file = new File(filePath);
            if(file.delete()){
                return true;
            }else{
                return false;
            }
        }catch(Exception e){
            return false;
        }
	}

	public static boolean copyFile(String sourcePath, String destPath) {
		File in = new File(sourcePath);
		File out = new File(destPath);
		if (!in.exists()) {
			Common.LOG.warn(in.getAbsolutePath() + " copy file not exists!");
			return false;
		}
		if (!out.exists()) {
			out.mkdirs();
		}
		FileInputStream fis = null;
		FileOutputStream fos = null;
		try {
			fis = new FileInputStream(in);
			fos = new FileOutputStream(new File(destPath + "\\" + in.getName()));
		} catch (Exception e) {
			 Common.LOG.error("copy file exception",e);
		}
		int c;
		byte[] b = new byte[1024 * 5];
		try {
			while ((c = fis.read(b)) != -1) {
				fos.write(b, 0, c);
			}
			fis.close();
			fos.flush();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static boolean copyFolder(String sourceFolder, String destFolder) {
		File in = new File(sourceFolder);
		File out = new File(destFolder);

		if (!in.exists()) {
			Common.LOG.warn(in.getAbsolutePath() + " copy folder not exists");
			return false;
		}
		if (!out.exists()) {
			out.mkdirs();
		}
		String[] file = in.list();
		FileInputStream fis = null;
		FileOutputStream fos = null;
		File temp = null;
		for (int i = 0; i < file.length; i++) {	 
			temp = new File(sourceFolder + "/" + file[i]);
			if (temp.isFile()) {
				try {
					fis = new FileInputStream(temp.getAbsolutePath());
					fos = new FileOutputStream(new File(destFolder + "/" + temp.getName()));
				} catch (Exception e) {
					Common.LOG.error("copy folder file exception",e);
				}
			}else if (temp.isDirectory()) {
				copyFolder(temp.getAbsolutePath(), destFolder + "/" +temp.getName());
			}
			int c;
			byte[] b = new byte[1024 * 5];
			try {
				while ((c = fis.read(b)) != -1) {
					fos.write(b, 0, c);
				}
				fis.close();
				fos.flush();
				fos.close();
			} catch (Exception e) {
				Common.LOG.error("copy folder exception",e);
			}
		}
		return false;
	} 
}
