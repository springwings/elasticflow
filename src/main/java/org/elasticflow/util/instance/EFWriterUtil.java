/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.util.instance;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.TimeZone;

import org.elasticflow.config.GlobalParam.MECHANISM;
import org.elasticflow.util.Common;
import org.elasticflow.config.InstanceConfig;

/**
 * EFWriterUtil
 * 
 * @author chengwen
 * @version 1.0
 * @date 2020-10-31 13:55
 * @modify 2021-01-10 09:45
 */
public class EFWriterUtil {

	public static int getWriterSocketIndex(InstanceConfig instanceConfig, int socketNum, long outTime) {
		if (instanceConfig.getPipeParams().getWriteMechanism() == MECHANISM.AB) {
			return 0;
		} else {
			int[] timeSpan = instanceConfig.getPipeParams().getKeepNums();
			LocalDate lnow;
			if (outTime > 0) {
				lnow = Instant.ofEpochMilli(outTime * 1000).atZone(ZoneOffset.ofHours(8)).toLocalDate();
			} else {
				lnow = LocalDate.now();
			}
			if (timeSpan[0] == 0) {
				return lnow.getDayOfMonth() % socketNum;
			} else {
				return lnow.getMonthValue() % socketNum;
			}
		}
	}

	/**
	 * writer with time Mechanism store id It only supports load balancing etc. at
	 * the top level of the resource
	 * 
	 * @param instanceConfig
	 * @return store id and need remove store-id
	 */
	public static EFTuple<Long, Long> timeMechanism(InstanceConfig instanceConfig) {
		int[] timeSpan = instanceConfig.getPipeParams().getKeepNums();
		Long storeId;
		long foward;
		long current = System.currentTimeMillis();
		LocalDate lnow = LocalDate.now();
		if (timeSpan[0] == 0) {
			foward = lnow.minusDays(timeSpan[1]).atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();
			storeId = (current / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset()) / 1000;
		} else {
			Long startDay = Common.getMonthStartTime(current, "GMT+8:00");
			storeId = startDay / 1000;
			LocalDate ldate = LocalDate.of(lnow.getYear(), lnow.getMonth(), 1);
			foward = ldate.minusMonths(timeSpan[1]).atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();
		}
		Long keepLastTime = foward / 1000;
		return new EFTuple<Long, Long>(storeId, keepLastTime);
	} 

}
