/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.computer.algorithm;

/**
 * 
 * @author chengwen
 * @version 1.0
 * @date 2018-11-22 09:04
 */
public final class MathFunction {

	public static double sigmoid(double v) {
		return 1.0 / (1.0 + Math.exp(v));
	}

	public static double pointMulti(double[] v1, double[] v2) {
		double res = 0d;
		for (int i = 0; i < v1.length; i++) {
			res += v1[i] * v2[i];
		}
		return res;
	}

	public static double sigmaSqrt(double[] v) {
		double res = 0d;
		for (int i = 0; i < v.length; i++) {
			res += v[i] * v[i];
		}
		return Math.sqrt(res);
	}

	public static double[] normalizeVector(double[] vector) {
		double sum = 0;
		for (double v : vector) 
			sum += v * v;

		double norm = Math.sqrt(sum);
		double[] normalizedVector = new double[vector.length];
		for (int i = 0; i < vector.length; i++) {
			normalizedVector[i] = vector[i] / norm;
		}
		return normalizedVector;
	}
}
