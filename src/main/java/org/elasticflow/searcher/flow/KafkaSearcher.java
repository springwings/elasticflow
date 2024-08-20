package org.elasticflow.searcher.flow;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticflow.config.GlobalParam;
import org.elasticflow.model.EFResponse;
import org.elasticflow.model.searcher.ResponseDataUnit;
import org.elasticflow.model.searcher.SearcherModel;
import org.elasticflow.model.searcher.SearcherResult;
import org.elasticflow.param.pipe.ConnectParams;
import org.elasticflow.searcher.SearcherFlowSocket;
import org.elasticflow.searcher.handler.SearcherHandler;
import org.elasticflow.util.Common;
import org.elasticflow.util.EFException;

/**
 * Main Run Class for Searcher
 * 
 * @author chengwen
 * @version 5.x
 * @date 2023-10-26 09:23
 */
public final class KafkaSearcher extends SearcherFlowSocket {

	private int count = 5;
	private Properties consumerConf; 
	
	public static boolean crossSubtasks = true;

	public static KafkaSearcher getInstance(ConnectParams connectParams) {
		KafkaSearcher o = new KafkaSearcher();
		o.connectParams = connectParams;
		Properties props = new Properties();
		props.put("bootstrap.servers", o.connectParams.getWhp().getHost());
		props.put("group.id", "test-consumer-group");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", StringDeserializer.class);
		props.put("max.poll.records", GlobalParam.READ_PAGE_SIZE);
		props.put("max.partition.fetch.bytes", 1048576 * 10);
		props.put("enable.auto.commit", "false"); // 设置为false，手动提交偏移量
		props.put("session.timeout.ms", "30000");
		props.put("heartbeat.interval.ms", "3000");
		props.put("auto.offset.reset", "latest");
		o.consumerConf = props;
		Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, o.connectParams.getWhp().getHost());
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"); 
		return o;
	}

	@Override
	public void Search(SearcherModel<?> searcherModel, String instance, SearcherHandler handler, EFResponse efResponse)
			throws EFException {
		SearcherResult res = new SearcherResult();
		String content = searcherModel.efRequest.getStringParam("content");
		String id = searcherModel.efRequest.getStringParam("id");
		count = searcherModel.getCount();
		String topic = this.connectParams.getWhp().getDefaultValue().getString("consumer.topic");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConf);
		try {
			consumer.subscribe(Arrays.asList(topic)); 
			while (consumer.assignment().isEmpty()) {
				consumer.poll(Duration.ofMillis(100));
			}
 			Set<TopicPartition> partitions = consumer.assignment();
			for (TopicPartition partition : partitions) {
				long lastOffset = consumer.endOffsets(Collections.singleton(partition)).get(partition);
				long expectedOffSet = Math.max(lastOffset - count, 0);
				consumer.seek(partition, expectedOffSet);
			}

			int nums = count;
			boolean check = false;
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
				for (ConsumerRecord<String, String> record : records) {
					if (!check) nums--;
					check = (content.length() > 0 && record.value().contains(content)) || (id.length()>0 && record.key().equals(id));
					if (check || (content.length() == 0 && id.length()==0)) {
						ResponseDataUnit u = ResponseDataUnit.getInstance();
						u.addObject(record.key(), record.value());
						res.getUnitSet().add(u);
					}
					count--;
					if (count <= 0) break;
				}
				if (count <= 0) break;
			}
			res.setTotalHit(nums);
		} catch (Exception e) {
			e.printStackTrace();
			throw Common.convertException(e);
		} finally {
			consumer.close();
		}
		this.formatResult(res, efResponse);
	} 
}
