package org.elasticflow.searcher.flow;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticflow.config.GlobalParam;
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
	private Properties adminConf;

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
		props.put("enable.auto.commit", "false");
		props.put("session.timeout.ms", "30000");
		props.put("heartbeat.interval.ms", "3000");
		props.put("auto.offset.reset", "latest");
		o.consumerConf = props;
		Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, o.connectParams.getWhp().getHost());
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		o.adminConf = conf;
		return o;
	}

	@Override
	public SearcherResult Search(SearcherModel<?> searcherModel, String instance, SearcherHandler handler)
			throws EFException {
		SearcherResult res = new SearcherResult();
		String content = searcherModel.efRequest.getStringParam("content");
		count = searcherModel.getCount();
		String topic = this.connectParams.getWhp().getDefaultValue().getString("consumer.topic");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConf);
		try {
			AdminClient adminClient = AdminClient.create(adminConf);
			DescribeTopicsResult topicResult = adminClient.describeTopics(Arrays
					.asList(this.connectParams.getWhp().getDefaultValue().getString("consumer.topic").split(",")));

			Map<String, KafkaFuture<TopicDescription>> descMap = topicResult.values();
			Iterator<Map.Entry<String, KafkaFuture<TopicDescription>>> itr = descMap.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry<String, KafkaFuture<TopicDescription>> entry = itr.next();
				List<TopicPartitionInfo> topicPartitionInfoList = entry.getValue().get().partitions();
				topicPartitionInfoList.forEach((e) -> {
					int partitionId = e.partition();
					TopicPartition topicPartition = new TopicPartition(topic, partitionId);
//                    Map<TopicPartition, Long> mapBeginning = consumer.beginningOffsets(Arrays.asList(topicPartition));
//                    Iterator<Map.Entry<TopicPartition, Long>> itr2 = mapBeginning.entrySet().iterator();
//                    long beginOffset = 0; 
//                    while(itr2.hasNext()) {
//                        Map.Entry<TopicPartition, Long> tmpEntry = itr2.next();
//                        beginOffset =  tmpEntry.getValue();
//                    }
					Map<TopicPartition, Long> mapEnd = consumer.endOffsets(Arrays.asList(topicPartition));
					Iterator<Map.Entry<TopicPartition, Long>> itr3 = mapEnd.entrySet().iterator();
					long lastOffset = 0;
					while (itr3.hasNext()) {
						Map.Entry<TopicPartition, Long> tmpEntry2 = itr3.next();
						lastOffset = tmpEntry2.getValue();
					}
					long expectedOffSet = lastOffset - count;
					if (expectedOffSet <= 0) {
						expectedOffSet = 1;
						count = 1;
					}
					consumer.commitSync(
							Collections.singletonMap(topicPartition, new OffsetAndMetadata(expectedOffSet - 1)));
				});
			}
			consumer.subscribe(Arrays.asList(topic));
			res.setTotalHit(count);
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
				for (ConsumerRecord<String, String> record : records) {
					if ((content.length() > 0 && record.value().contains(content)) || content.length() == 0) {
						ResponseDataUnit u = ResponseDataUnit.getInstance();
						u.addObject(record.key(), record.value());
						res.getUnitSet().add(u);
					}
					count--;
					if (count <= 0)
						break;
				}
				if (count <= 0)
					break;
			}
		} catch (Exception e) {
			throw Common.convertException(e);
		} finally {
			consumer.close();
		}
		return res;
	}

}
