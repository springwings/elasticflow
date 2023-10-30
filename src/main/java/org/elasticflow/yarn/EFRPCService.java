package org.elasticflow.yarn;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.elasticflow.util.EFException;
import org.elasticflow.util.EFException.ELEVEL;
import org.elasticflow.util.EFException.ETYPE;

/**
 * RPC Remote Proxy object Service
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-19 15:33
 */
public class EFRPCService<T> {

	@SuppressWarnings("unchecked")
	public static <T> T getRemoteProxyObj(final Class<?> serviceInterface, final InetSocketAddress addr) {
		return (T) Proxy.newProxyInstance(serviceInterface.getClassLoader(), new Class<?>[] { serviceInterface },
				new InvocationHandler() {
					public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
						Socket socket = new Socket();
						ObjectOutputStream output = null;
						ObjectInputStream input = null;
						try {
							socket.connect(addr);
							output = new ObjectOutputStream(socket.getOutputStream());
							output.writeUTF(serviceInterface.getName());
							output.writeUTF(method.getName());
							output.writeObject(method.getParameterTypes());
							output.writeObject(args);
							input = new ObjectInputStream(socket.getInputStream());
							return input.readObject();
						} catch (Exception e) {
							throw new EFException(e, "from node " + addr.getHostString() + " run method " + serviceInterface.getName()
									+ " > " + method.getName() + " exception!", ELEVEL.Dispose, ETYPE.RESOURCE_ERROR);
						} finally {
							if (socket.isConnected()) {
								try {
									if (socket != null)
										socket.close();
									if (output != null)
										output.close();
									if (input != null)
										input.close();
								} catch (Exception e) {
									throw new EFException(e);
								}
							}
						}
					}
				});
	}
}