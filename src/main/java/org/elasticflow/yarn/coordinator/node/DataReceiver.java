/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.yarn.coordinator.node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticflow.service.EFRPCService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See basic data transmission class for cluster nodes
 * 
 * @author chengwen
 * @version 0.1
 * @create_time 2021-07-30
 */
public class DataReceiver implements EFRPCService {

	private static ExecutorService executor = Executors.newFixedThreadPool(5);

	private static HashMap<String, Class<?>> serviceRegistry = new HashMap<String, Class<?>>();

	private boolean isRunning = false;

	private int port;

	private final static Logger log = LoggerFactory.getLogger(DataReceiver.class);

	public DataReceiver(int port) {
		this.port = port;
	}

	public void stop() {
		isRunning = false;
		executor.shutdown();
	}

	public void start() throws IOException {
		ServerSocket serverSocket = new ServerSocket();
		serverSocket.bind(new InetSocketAddress(this.port));
		try {
			while (true) {
				executor.execute(new Receiver(serverSocket.accept()));
			}
		} finally {
			serverSocket.close();
		}
	}

	public void register(Class<?> serviceInterface, Class<?> serviceImpl) {
		serviceRegistry.put(serviceInterface.getName(), serviceImpl);
	}

	public boolean isRunning() {
		return isRunning;
	}

	public int getPort() {
		return port;
	}

	/**
	 * Message synchronization 
	 */
	private static class Receiver implements Runnable {
		
		Socket socket = null;

		public Receiver(Socket socket) {
			this.socket = socket;
		}

		public void run() {
			ObjectOutputStream output = null;
			String methodName = null;
			String serviceName = null;
			try (ObjectInputStream input = new ObjectInputStream(socket.getInputStream());) {
				serviceName = input.readUTF();
				methodName = input.readUTF();
				Class<?>[] parameterTypes = (Class<?>[]) input.readObject();
				Object[] arguments = (Object[]) input.readObject();
				Class<?> serviceClass = serviceRegistry.get(serviceName);
				if (serviceClass == null) {
					log.warn("service {} is null.", serviceName);
				} else {
					Method method = serviceClass.getMethod(methodName, parameterTypes);
					Object result = method.invoke(serviceClass.getDeclaredConstructor().newInstance(), arguments);
					output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(result);
				}
			} catch (Exception e) {
				log.error("from node {} run method {} > {} exception!", socket.getInetAddress(),serviceName,methodName,e);
			} finally {
				if (output != null) {
					try {
						output.close();
					} catch (Exception e) {
						log.error("message receiver output stream close exception", e);
					}
				}
				if (socket != null) {
					try {
						socket.close();
					} catch (Exception e) {
						log.error("message receiver socket close Exception", e);
					}
				}
			}
		}
	}
}
