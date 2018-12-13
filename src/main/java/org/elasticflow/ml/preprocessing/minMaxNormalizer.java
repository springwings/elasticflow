package org.elasticflow.ml.preprocessing;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-13 10:34
 */
public class minMaxNormalizer {
 
	public static double[][] normalize(double[][] points) {
		if (points == null || points.length < 1) {
			return points;
		}
		double[][] p = new double[points.length][points[0].length];
		double[] matrixJ;
		double maxV;
		double minV;
		for (int j = 0; j < points[0].length; j++) {
			matrixJ = getMatrixCol(points, j);
			maxV = maxV(matrixJ);
			minV = minV(matrixJ);
			for (int i = 0; i < points.length; i++) {
				p[i][j] = maxV == minV ? minV : (points[i][j] - minV) / (maxV - minV);
			}
		}
		return p;
	}
 
	public static double[] getMatrixCol(double[][] points, int column) {
		double[] matrixJ = new double[points.length];
		for (int i = 0; i < points.length; i++) {
			matrixJ[i] = points[i][column];
		}
		return matrixJ;
	}

 
	public static double minV(double[] matrixJ) {
		double v = matrixJ[0];
		for (int i = 0; i < matrixJ.length; i++) {
			if (matrixJ[i] < v) {
				v = matrixJ[i];
			}
		}
		return v;
	}
 
	public static double maxV(double[] matrixJ) {
		double v = matrixJ[0];
		for (int i = 0; i < matrixJ.length; i++) {
			if (matrixJ[i] > v) {
				v = matrixJ[i];
			}
		}
		return v;
	}
}
