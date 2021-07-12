package org.elasticflow.util;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zookeeper utils
 * @author chengwen
 * @version 2.0
 * @date 2018-10-26 09:14
 */
public class ZKUtil {

	private static final int CONNECTION_TIMEOUT = 50000;
	private final static CountDownLatch connectedSemaphore = new CountDownLatch(
			1);
	private static String zkHost = null;
	private static ZooKeeper zk = null;
	private static Watcher watcher = null;
	private final static Logger log = LoggerFactory.getLogger(ZKUtil.class);

	public static ZooKeeper getZk() { 
		synchronized (ZKUtil.class) {
			if (zk == null || zk.getState().equals(States.CLOSED)) {
				connection();
			}
		} 
		return zk;
	}

	private static void connection() {
		try {
			watcher = new Watcher() {
				public void process(WatchedEvent event) {
					connectedSemaphore.countDown(); 
				}
			};
			zk = new ZooKeeper(zkHost, CONNECTION_TIMEOUT, watcher);
			connectedSemaphore.await();
		} catch (Exception e) {
			log.error("connection Exception", e);
		}
	}

	public static void setZkHost(String zkString) {
		zkHost = zkString.intern();
	} 
	
	public static String createPath(String path,boolean PERSISTENT){
		try{ 
			if(getZk().exists(path, true)==null){
				if(PERSISTENT){
					return getZk().create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}else{
					return getZk().create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				}
			} 
		} catch (Exception e) {
			log.error("createPath Exception", e);
		}
		return null;
	}
	
	public static void removePath(String path){ 
        try {  
            List<String> nodes = getZk().getChildren(path, false);  
            for (String node : nodes) {  
                zk.delete(path + "/" + node, -1);  
            }  
            zk.delete(path, -1);  
        } catch (Exception e) {  
        	log.error("removePath Exception", e);
        }  
	}

	public static void setData(String filename, String Content) {
		byte[] bt = Content.getBytes();
		try {
			getZk().setData(filename, bt, -1);
		} catch (Exception e) {
			log.error("setData Exception", e);
		}
	}

	public static byte[] getData(String filename,boolean create) {
		try {
			return getZk().getData(filename, watcher, null);
		} catch (Exception e) {
			if(e.getMessage().contains("NoNode") && create) {
				createPath(filename, true);
				return "".getBytes();
			}
			log.error("getData Exception",e);
			return null;
		}
	}
	
	private static void watchNode(final String path,final Thread current) throws InterruptedException {
        try {
            zk.exists(path, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) { 
                    if(watchedEvent.getType() == Event.EventType.NodeDeleted){ 
                    	current.interrupt();
                    } 
                    try {
                        zk.exists(path,new Watcher() {
                            @Override
                            public void process(WatchedEvent watchedEvent) { 
                                if(watchedEvent.getType() == Event.EventType.NodeDeleted){ 
                                	current.interrupt();
                                }
                                try {
                                    zk.exists(path,true);
                                } catch (Exception e) {
                                	log.error("watchNode Exception", e); 
                                }  
                            }

                        });
                    } catch (Exception e) {
                    	log.error("watchNode Exception", e); 
                    } 
                }

            });
        } catch (Exception e) {
        	log.error("watchNode Exception", e); 
        }
    }
 
	/**
	 * distribute locks
	 * @param path
	 * @return
	 * @throws InterruptedException
	 */
    public static boolean getDistributeLock(String path) throws InterruptedException { 
        watchNode(path,Thread.currentThread());
        while (true) {
        	String res;
        	try {
        		res = getZk().create(path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        	} catch (Exception e) { 
				log.warn("Waiting get lock!");
				res=null;
			}  
        	try {
                Thread.sleep(3000);
            }catch (Exception e){
            	log.error("lock InterruptedException", e); 
            }
            if (res!=null && !res.equals("null")) { 
                return true;
            }
        }
    } 
    
    public static void unDistributeLock(String path){
        try {
        	removePath(path);  
        } catch (Exception e) {
        	log.error("unlock Exception", e); 
        } 
    } 
}
