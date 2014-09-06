package com.poc.trident.topology.examples;

import java.io.IOException;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

import com.poc.trident.fake.generator.FakeLogGeneratorBatchSpout;
import com.poc.trident.fake.generator.Utils;
import com.poc.trident.topology.cluster.KafkaMessageProcessor;

import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

public class GeoPubAggByMinuteTopology {

	// "id", "geo", "pub", "adv", "website","bid","cookie","date";
	public static StormTopology buildTopology(LocalDRPC drpc)
			throws IOException {

		BrokerHosts brokerHosts = new ZkHosts(
				"ec2-54-237-148-55.compute-1.amazonaws.com:2181");
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts,
				"adnetwork-topic");
        kafkaConfig.bufferSizeBytes = 1024 * 1024 * 4;
		kafkaConfig.fetchSizeBytes = 1024 * 1024 * 4;
		kafkaConfig.forceFromStart = false;
     	kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		//FakeLogGeneratorBatchSpout spout = new FakeLogGeneratorBatchSpout(100);
		
		TransactionalTridentKafkaSpout txKafak = new TransactionalTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();
	       topology.newStream("spout", txKafak)
				.each(new Fields("str"),new KafkaMessageProcessor(),new Fields("batch_out"));

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setNumWorkers(4);
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("logAnalysis", conf, buildTopology(drpc));
	}
}
