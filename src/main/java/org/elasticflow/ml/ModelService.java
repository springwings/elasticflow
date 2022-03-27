package org.elasticflow.ml;

import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Set;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Model Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */
public class ModelService extends ComputerFlowSocket {

	private CopyOnWriteArrayList<Socket> clients = new CopyOnWriteArrayList<>();
	
	private HashMap<String, Process> processes = new HashMap<>();
	 
	private String pyTemplete;
	
	private int portNum = 100;
	
	private final static Logger log = LoggerFactory.getLogger("ML");

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
		synchronized(this.clients) {
			if(this.clients.size()==0) {
				this.addService(39000+portNum,context.getInstanceConfig().getComputeParams().getPyPath());
				portNum++;
			}
		}
		if (this.computerHandler != null) {
			this.computerHandler.handleData(this, context, DSR);
		} else {
			while (DSR.nextLine()) {
				PipeDataUnit pdu = DSR.getLineData();
				PipeDataUnit u = PipeDataUnit.getInstance();
				Set<Entry<String, Object>> itr = pdu.getData().entrySet();
				for (Entry<String, Object> k : itr) {
					this.send(k.getValue());
				}
				this.dataUnit.add(u);
			}
			this.dataPage.put(GlobalParam.READER_LAST_STAMP, DSR.getScanStamp());
			this.dataPage.putData(this.dataUnit);
			this.dataPage.putDataBoundary(DSR.getDataBoundary());
		}
		return this.dataPage;
	}

	private void addService(int port,String pyPath) {
		try {
			this.runService(port,pyPath);
			Socket client = new Socket("127.0.0.1", port);
			clients.add(client);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	private String getPyName(int port) {
		return "EF_TEMP_"+String.valueOf(port)+".py";
	}

	private Socket getSocket() {
		return clients.get(0);
	}

	private void runService(int port,String pyPath) { 
		try { 
			String pyname = getPyName(port);
			String runPy = pyPath+"/"+pyname; 
			EFFileUtil.createAndSave(pyTemplete, runPy);
			ProcessBuilder pb = new ProcessBuilder("python3", runPy, String.valueOf(port)).inheritIO();
			Process proc = pb.start();
			processes.put(pyname, proc);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	private void send(Object data) {
		Socket client = getSocket();
		try (OutputStreamWriter os = new OutputStreamWriter(client.getOutputStream());) {
			os.write(data.toString());
			os.flush();
			InputStream is = client.getInputStream();
			byte[] buf = new byte[1024 * 8];
			StringBuilder recive = new StringBuilder();
			for (int len = is.read(buf); len > 0; len = is.read(buf)) {
				recive.append(new String(buf, 0, len));
			}
			client.shutdownInput();
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

}
