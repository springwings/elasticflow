package org.elasticflow.util;

import java.security.MessageDigest;

/**
 * 
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:21
 */
public class MD5Util {
	public final static String SaltMd5(String strs){
		return MD5(MD5(strs).substring(5, 8)+strs+MD5(strs).substring(0,5));
	}
	public final static String MD5(String strs) {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'A', 'B', 'C', 'D', 'E', 'F' };
		try {
			byte[] btInput = (strs+"_SERACH").getBytes(); 
			MessageDigest mdInst = MessageDigest.getInstance("MD5"); 
			mdInst.update(btInput); 
			byte[] md = mdInst.digest(); 
			int j = md.length;
			char str[] = new char[j * 2];
			int k = 0;
			for (int i = 0; i < j; i++) {
				byte byte0 = md[i];
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];
				str[k++] = hexDigits[byte0 & 0xf];
			} 
			return new String(str);
		} catch (Exception e) {
			e.printStackTrace(); 
		}
		return "";
	}
	
	public static void main(String[] args) {
		System.out.println(SaltMd5("image"));
	}
}
