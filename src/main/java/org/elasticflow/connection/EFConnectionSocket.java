package org.elasticflow.connection;

import org.elasticflow.config.GlobalParam.END_TYPE;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.util.EFException;

/**
 * Connect Abstract Interface When implementing inheritance, it is necessary to
 * pay attention to the file name, which should be a hump and can only be two
 * uppercase letters
 * 
 * @author chengwen
 * @version 1.0
 * @param <T>
 * @date 2018-10-24 13:53
 */
public abstract class EFConnectionSocket<T> {

	protected volatile ConnectParams connectParams;

	private boolean isShare = false;

	/**
	 * Store special error information in the connection for feedback to the upper
	 * level during debugging
	 */
	private String infos = "";

	/** Control whether the connection has expired. */
	private long version;

	protected T conn;

	public abstract boolean connect(END_TYPE endType);

	public abstract boolean status();

	public abstract boolean free();

	public void init(ConnectParams ConnectParams) {
		this.connectParams = ConnectParams;
	}

	public boolean isShare() {
		return this.isShare;
	}

	public void setShare(boolean share) {
		this.isShare = share;
	}

	public ConnectParams getConnectParams() {
		return this.connectParams;
	}

	public T getConnection(END_TYPE endType) throws EFException {
		int tryTime = 0;
		while (tryTime < 5 && !connect(endType)) {
			tryTime++;
			try {
				Thread.sleep(1000 + tryTime * 500);
			} catch (InterruptedException e) {
				throw new EFException(e);
			}
		}
		return this.conn;
	}

	public void setInfos(String infos) {
		this.infos = infos;
	}

	public String getInfos() {
		String tmp = this.infos;
		this.infos = "";
		return tmp;
	}

	public long getVersion() {
		return version;
	}

	public void setVersion(long version) {
		this.version = version;
	}
}
