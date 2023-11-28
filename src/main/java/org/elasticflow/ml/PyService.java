package org.elasticflow.ml;

import java.io.InputStream;
import java.net.Socket;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticflow.computer.ComputerFlowSocket;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.instruction.Context;
import org.elasticflow.model.EFSocket;
import org.elasticflow.model.reader.DataPage;
import org.elasticflow.model.reader.PipeDataUnit;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.reader.util.DataSetReader;
import org.elasticflow.util.EFException;
import org.elasticflow.util.EFFileUtil;
import org.elasticflow.yarn.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * python recall Compute
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-05-22 09:08
 */

public class PyService extends ComputerFlowSocket {

	private CopyOnWriteArrayList<EFSocket> clients = new CopyOnWriteArrayList<>();
	
	private CopyOnWriteArrayList<Process> processes = new CopyOnWriteArrayList<>();
	 
	private String pyTemplete;
	
	static AtomicInteger portNum = new AtomicInteger(100);
	
	private final static Logger log = LoggerFactory.getLogger("PyService");

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
		if (this.computerHandler != null) {
			this.computerHandler.handleData(this, context, DSR);
		} else {
			if(this.clients.size()==0) {
				this.addService(39000+portNum.getAndIncrement(),context.getInstanceConfig().getComputeParams().getPyPath());
			}
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
	
	public void shutdown() {
		for(EFSocket sk : clients) {
			try {
				sk.close();
			} catch (Exception e) {
				log.warn("shut down model service socket exception",e);
			}
		}
		for(Process proc:processes) {
			proc.destroy();
		}
	}

	private synchronized void addService(int port,String pyPath) {
		Resource.threadPools.execute(() -> { 
			this.runService(port,pyPath); 
		});		
		for(int i=0;i<5;i++) {
			boolean success = connectSocket(port);
			if(success)
				break;
		}
	} 
	
	private boolean connectSocket(int port) {
		try {
			Thread.sleep(200);
			Socket sk = new Socket("127.0.0.1", port);
			clients.add(new EFSocket(sk));
		} catch (Exception e) {
			return false;
		} 		
		return true;
	}
	
	private String getPyName(int port) {
		return "EF_TEMP_"+String.valueOf(port)+".py";
	}

	private EFSocket getSocket() {
		return clients.get(0);
	}

	private void runService(int port,String pyPath) { 
		try { 
			String pyname = getPyName(port);
			String runPy = pyPath+"/"+pyname; 
			EFFileUtil.createAndSave(pyTemplete, runPy);
			ProcessBuilder pb = new ProcessBuilder("python3", runPy, String.valueOf(port)).inheritIO();
			Process proc = pb.start();
			processes.add(proc);
			proc.waitFor();
		} catch (Exception e) {
			log.error("open model service socket exception",e);
		}
	}

	private void send(Object data) {
		EFSocket client = getSocket();
		try {
			client.getOSW().write(data.toString());
			client.getOSW().flush();
			InputStream is = client.getSocket().getInputStream();
			byte[] buf = new byte[1024 * 8];
			StringBuilder recive = new StringBuilder();
			for (int len = is.read(buf); len > 0; len = is.read(buf)) {
				recive.append(new String(buf, 0, len));
			}
			System.out.println(recive);
		} catch (Exception e) {
			log.error("send message exception",e);
		}
	}
}
