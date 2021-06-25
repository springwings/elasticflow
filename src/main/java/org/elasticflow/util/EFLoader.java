/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.elasticflow.config.GlobalParam;

/**
 * Auto load class in runtime
 * @author chengwen
 * @version 1.0
 * @date 2019-01-15 11:07
 * @modify 2019-01-15 11:07
 */
public class EFLoader extends ClassLoader {

	private String path = GlobalParam.configPath;  
	
	public EFLoader(String path) {
		super();
		this.path = path;
	}

	public EFLoader(String path,ClassLoader parent) {
		super(parent);
		this.path = path;
	}

	@Override
	public Class<?> findClass(String name) throws ClassNotFoundException { 
		byte[] b = loadClassData();
		return defineClass(name, b, 0, b.length);
	}

	private byte[] loadClassData() {
		byte[] data = null;
		InputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			in = new FileInputStream(new File(path));
			int len = 0;
			while (-1 != (len = in.read())) {
				out.write(len);
			}
			data = out.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return data;
	}
}
