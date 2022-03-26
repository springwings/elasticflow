package org.elasticflow.ml.algorithm;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;

/**
 * Model Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ModelService extends ComputerFlowSocket {

	private CopyOnWriteArrayList<Socket> clients = new CopyOnWriteArrayList<>();
	 
	private String pyTemplete;

	public static ModelService getInstance(final ConnectParams connectParams) {
		ModelService o = new ModelService();
		o.initConn(connectParams);
		o.init();
		return o;
	}
	
	public void init() {
		pyTemplete = EFFileUtil.readText("src/main/resources/ModelService.py", "utf-8", false);
	}
	
	@Override
	public DataPage predict(Context context, DataSetReader DSR) throws EFException {
		while (DSR.nextLine()) {
			PipeDataUnit pdu = DSR.getLineData();
		}
		return null;
	}

	private void addClient(int port) {
		try {
			Socket client = new Socket("127.0.0.1", port);
			clients.add(client);
		} catch (Exception e) {
		}
	}

	private Socket getSocket() {
		return clients.get(0);
	}

	private void runService(String pyPath) { 
		try { 
			Process proc = Runtime.getRuntime().exec("python3 " + pyPath); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void send(String data) {
		Socket client = getSocket();
		try (OutputStreamWriter os = new OutputStreamWriter(client.getOutputStream());) {
			os.write(data);
			os.flush();
			InputStream is = client.getInputStream();
			byte[] buf = new byte[1024 * 8];
			StringBuilder recive = new StringBuilder();
			for (int len = is.read(buf); len > 0; len = is.read(buf)) {
				recive.append(new String(buf, 0, len));
			}
			client.shutdownInput();
		} catch (Exception e) {

		}
	}

}
