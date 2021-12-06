package org.elasticflow.service;

import java.io.IOException;

public interface EFRPCService {
	
	public void stop();
	 
    public void start() throws IOException;
 
    public void register(Class<?> serviceInterface, Class<?> impl);
 
    public boolean isRunning();
 
    public int getPort();
    
}
