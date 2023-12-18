package org.elasticflow.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.ArrayList;

import org.elasticflow.config.GlobalParam;

/**
 * read/delete/copy/create/scan/get filenmae and extension
 * @author chengwen
 * @version 1.0
 * @date 2018-11-07 14:12
 */
public class EFFileUtil {

	public static String[] getInstancePath(String instance) {
		String[] dt = new String[3];
		dt[0] = GlobalParam.INSTANCE_PATH + "/" + instance + "/" + GlobalParam.JOB_INCREMENTINFO_PATH;
		dt[1] = GlobalParam.INSTANCE_PATH + "/" + instance + "/task.xml";
		dt[2] = GlobalParam.INSTANCE_PATH + "/" + instance + "/stat";
		return dt;
	}

	public static String readText(String filePath, String encoding, boolean create) {
		File file = new File(filePath);
		Long filelength = file.length();
		byte[] filecontent = new byte[filelength.intValue()];
		try (FileInputStream in = new FileInputStream(file)) {
			in.read(filecontent);
			return new String(filecontent, encoding);
		} catch (FileNotFoundException e1) {
			if (create) {
				createAndSave("", filePath);
			} else {
				Common.LOG.warn("read text Exception", e1);
			}
		} catch (Exception e2) {
			Common.LOG.warn("read text Exception", e2);
		}
		return null;
	}

	public static void renameFile(String filePath, String oldName, String newName) {
		if (!oldName.equals(newName)) {
			File oldFile = new File(filePath + "/" + oldName);
			File newFile = new File(filePath + "/" + newName);
			if (newFile.exists()) {
				Common.LOG.warn(newName + " the file already exists！");
			} else {
				oldFile.renameTo(newFile);
			}
		}
	}

	public static boolean delFile(String filePath) {
		try {
			File file = new File(filePath);
			if (file.delete()) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean createAndSave(String content, String fileDest) {
		FileWriter writer = null;
		try {
			File file = new File(fileDest);
			if (!file.exists()) {
				File parentFile = file.getParentFile();
				if (parentFile.exists()) {
					file.createNewFile();
				} else {
					parentFile.mkdirs();
					file.createNewFile();
				}
			}
			writer = new FileWriter(file, false);
			writer.write(content);
		} catch (Exception e) {
			Common.LOG.warn("create File Exception", e);
			return false;
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (Exception e) {
				Common.LOG.warn("writer close Exception", e);
				return false;
			}
		}
		return true;
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
			Common.LOG.error("copy file {} exception",sourcePath, e);
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
	
	/**
	 * get file lists
	 * @param folder
	 * @param extension  such as ".csv"
	 * @return
	 */
	public static ArrayList<String> scanFolder(String folder,String extension) {
		ArrayList<String> res = new ArrayList<>();
		File s1 = new File(folder);
		File[] str = s1.listFiles();
		for (File i : str) {
			if (i.isFile()) {
				if(extension != null) {
					if (i.getName().endsWith(extension))
						res.add(i.getPath());
				}else {
					res.add(i.getPath());
				}
			}
		}
		return res;
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
					Common.LOG.error("copy folder {} exception", sourceFolder,e);
				}
			} else if (temp.isDirectory()) {
				copyFolder(temp.getAbsolutePath(), destFolder + "/" + temp.getName());
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
				Common.LOG.error("copy folder {} exception",sourceFolder, e);
			}
		}
		return false;
	}
	
	/**
	 * Resolve file suffix names based on file path
	 * @param fpath
	 * @return
	 */
	public static String getFileExtension(String fpath) {
		String[] strArray = fpath.toLowerCase().split("\\.");
		int suffixIndex = strArray.length -1;
		return strArray[suffixIndex];
	}
	
	/**
	 * get lastModified,getName,...
	 * @param fpath
	 * @return
	 */
	public static File getFileObj(String fpath) {
		File file = new File(fpath); 
		return file;
	}
	
	public static boolean checkResourceExists(String path) {
		File file = new File(path);
		if (file.exists()) {
			return true;
		}
		return false;
	}
	
	public static String getJarDir(){ 
		try {
			String jarPath = EFFileUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			return new File(jarPath).getParent();
		} catch (Exception e) { 
			Common.LOG.warn("get jar dir exception",e);
			return "/opt/EF";
		} 
	}
}
