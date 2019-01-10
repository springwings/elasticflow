package org.elasticflow.util;

import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-10-26 09:15
 */
public class IK {
	 static IKAnalyzer analyzer;
	 static {
		 analyzer = new IKAnalyzer(); 
		 analyzer.setUseSmart(true);
	 }
	 
	 public static TokenStream participle(String contents) {
		 TokenStream result = null;
		 try {
			 result = analyzer.tokenStream("contents",  new StringReader(contents));
		 }catch (Exception e) {
			Common.LOG.error("IK TokenStream Exception ",e);
		 } 
		 return result;
	 }
	 
}
