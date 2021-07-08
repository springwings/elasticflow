/*
 * Copyright ElasticFlow B.V. and/or licensed to ElasticFlow B.V. under one
 * or more contributor license agreements. Licensed under the ElasticFlow License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the ElasticFlow License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticflow.computer;

import java.io.File;
import java.io.FileInputStream;

import org.elasticflow.config.InstanceConfig;
import org.elasticflow.flow.Flow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.djl.Model;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.modality.cv.transform.CenterCrop;
import ai.djl.modality.cv.transform.Normalize;
import ai.djl.modality.cv.transform.Resize;
import ai.djl.modality.cv.transform.ToTensor;
import ai.djl.modality.cv.translator.ImageClassificationTranslator;
import ai.djl.translate.Pipeline;
import ai.djl.translate.Translator;

public abstract class ComputerFlowSocket extends Flow {

	private Predictor<Image, Classifications> predictor;

	private final static Logger log = LoggerFactory.getLogger(ComputerFlowSocket.class);

	public void INIT(InstanceConfig instanceConfig) {
		Pipeline pipeline = new Pipeline();
		pipeline.add(new Resize(256)).add(new CenterCrop(224, 224)).add(new ToTensor())
				.add(new Normalize(new float[] { 0.485f, 0.456f, 0.406f }, new float[] { 0.229f, 0.224f, 0.225f }));
		Translator<Image, Classifications> translator = ImageClassificationTranslator.builder().setPipeline(pipeline)
				.optApplySoftmax(true).build();
		Model model = Model.newInstance("");
		try {
			File fs = new File("");
			model.load(fs.toPath());
			this.predictor = model.newPredictor(translator);
		} catch (Exception e) {
			log.error("INIT Computer Flow Socket Exception", e);
		}
	}

	public void predict(String info) {
		try {
			File fs = new File(info);
			Image img = ImageFactory.getInstance().fromInputStream(new FileInputStream(fs));
			this.predictor.predict(img);
		} catch (Exception e) {
			log.error("predict Exception", e);
		}
	}
}
