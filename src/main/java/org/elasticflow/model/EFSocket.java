package org.elasticflow.model;

import java.io.OutputStreamWriter;
import java.net.Socket;

public class EFSocket {
	
	private OutputStreamWriter OSW;
	
	private Socket SK;
	
	public EFSocket(Socket SK) throws Exception {
		this.OSW = new OutputStreamWriter(SK.getOutputStream());;
		this.SK = SK;
	}
	
	public OutputStreamWriter getOSW() {
		return this.OSW;
	}
	
	public Socket getSocket() {
		return this.SK;
	}
	
	public void close() {
		
	}
}
