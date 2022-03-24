package org.elasticflow.piper;

import java.util.Random;

/**
 * Flow size control
 * 
 * @author chengwen
 * @version 1.0
 * @date 2019-01-24 10:55
 * @modify 2019-01-24 10:55
 */
public class Valve {
	
	private int turnLevel;
	
	public Valve() {
		turnLevel = 9;
	}
	
	public int getTurnLevel() {
		return turnLevel;
	}
	
	public void setTurnLevel(int turnLevel) {
		this.turnLevel = turnLevel;
	}

	/**
	 * if is on,close flow
	 * @return
	 */
	public boolean isOn() {
		int seed = new Random().nextInt(9)+1;
		if(seed<=turnLevel)
			return false;
		return true;
	}
}
