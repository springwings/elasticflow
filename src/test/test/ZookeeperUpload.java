package test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZookeeperUpload {

	private static String window_spilt = "\\";
	private static String linux_spilt = "/";
	private final static int BUFFER_LEN = 1024;
	private final static int END = -1;

	private static void moveFile2Zookeeper(String sourceAdd,
			String destinationAdd, String zookeeperAdd) {
		ZooKeeper zk = null;
		Stat stat = null;

		try {
			Watcher watcher = new Watcher() {
				public void process(WatchedEvent event) {
					// System.out.println("已经触发了" + event.getType() + "事件！");
				}
			};
			zk = new ZooKeeper(zookeeperAdd, 5000, watcher);

			stat = zk.exists(destinationAdd, watcher);
			if (null == stat) {
				zk.create(destinationAdd, "".getBytes(), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			}
			moveFile(sourceAdd, zk, watcher, destinationAdd);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != zk) {
					zk.close();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	private static void moveFile(String sourceAdd, ZooKeeper zk,
			Watcher watcher, String destinationAdd) {

		InputStream in = null;
		Stat stat = null;
		String remoteAdd = null;
		try {
			File file = new File(sourceAdd);

			if (!file.isDirectory()) {
				in = new FileInputStream(sourceAdd);
				remoteAdd = destinationAdd + linux_spilt + file.getName();
				stat = zk.exists(remoteAdd, watcher);
				if (null == stat) {
					zk.create(remoteAdd, "".getBytes(), Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
				StringBuffer sb = new StringBuffer();
				byte[] buffer = new byte[BUFFER_LEN];
				while (true) {
					int byteRead = in.read(buffer);
					if (byteRead == END)
						break;
					sb.append(new String(buffer, 0, byteRead));
				}
				zk.setData(remoteAdd, sb.toString().getBytes(), -1);
			} else {
				String[] filelist = file.list();
				for (int i = 0; i < filelist.length; i++) {
					File readfile = new File(sourceAdd + window_spilt
							+ filelist[i]);
					if (!readfile.isDirectory()) {
						in = new FileInputStream(sourceAdd + window_spilt
								+ filelist[i]);

						stat = zk.exists(destinationAdd, watcher);
						if (null == stat) {
							zk.create(destinationAdd, "".getBytes(),
									Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						remoteAdd = destinationAdd + linux_spilt
								+ file.getName();
						stat = zk.exists(remoteAdd, watcher);
						if (null == stat) {
							zk.create(remoteAdd, "".getBytes(),
									Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						remoteAdd = destinationAdd + linux_spilt
								+ file.getName() + linux_spilt
								+ readfile.getName();

						stat = zk.exists(remoteAdd, watcher);
						if (null == stat) {
							zk.create(remoteAdd, "".getBytes(),
									Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						StringBuffer sb = new StringBuffer();
						byte[] buffer = new byte[BUFFER_LEN];
						while (true) {
							int byteRead = in.read(buffer);
							if (byteRead == END)
								break;
							sb.append(new String(buffer, 0, byteRead));
						}
						zk.setData(remoteAdd, sb.toString().getBytes(), -1);
						System.out.println("文件" + remoteAdd + " 已处理！");
					} else if (readfile.isDirectory()) {// 这里是按照递归处理的
						String sourceAdd2 = sourceAdd + window_spilt
								+ readfile.getName();
						String destinationAdd2 = destinationAdd + linux_spilt
								+ file.getName();
						moveFile(sourceAdd2, zk, watcher, destinationAdd2);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void runBatch(String zkhost, String add, String listProj) {
		String zkpath = "/EF/config/INSTANCES";
		for (String s : listProj.split(",")) {
			if (s.length() > 1) {
				moveFile2Zookeeper(add + s, zkpath, zkhost);
			}
		}

	}

	public static void main(String[] args) { 
		String zkhost = null, add = null;
		boolean batch = false;
		zkhost = "10.202.251.141:2181";
		add = "E:\\svn\\config\\beta\\statisticsInstanceData\\test";

		if (add != null && zkhost != null) {
			String zkpath = "/statisticsplatform/config/INSTANCES";
			if (batch) {
				runBatch(zkhost, add, "instances");
			} else {
				moveFile2Zookeeper(add, zkpath, zkhost);
			}
		}

	}
}
