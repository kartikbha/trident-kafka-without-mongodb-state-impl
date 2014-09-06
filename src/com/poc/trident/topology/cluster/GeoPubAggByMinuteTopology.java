package com.poc.trident.topology.cluster;

import java.io.IOException;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

import com.poc.trident.fake.generator.Utils;

public class GeoPubAggByMinuteTopology {

	// "id", "geo", "pub", "adv", "website","bid","cookie","date";
	public static StormTopology buildTopology(LocalDRPC drpc)
			throws IOException {

		BrokerHosts brokerHosts = new ZkHosts(
				"ec2-54-237-148-55.compute-1.amazonaws.com:2181");
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts,
				"adnetwork-topic");
		kafkaConfig.bufferSizeBytes = 1024 * 1024 * 20;
		kafkaConfig.fetchSizeBytes = 1024 * 1024 * 20;
		kafkaConfig.forceFromStart = false;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// FakeLogGeneratorBatchSpout spout = new
		// FakeLogGeneratorBatchSpout(100);

		TransactionalTridentKafkaSpout txKafak = new TransactionalTridentKafkaSpout(
				kafkaConfig);

		TridentTopology topology = new TridentTopology();
		topology.newStream("spout", txKafak)
				.each(new Fields("str"),
						new KafkaMessageProcessor(),
						new Fields("geo", "pub", "adv", "website", "bid",
								"cookie", "date"))
				.each(new Fields("geo", "pub", "adv", "website", "bid",
						"cookie", "date"), new Utils.LocationFilter("unknown"))
				.each(new Fields("geo", "pub", "adv", "website", "bid",
						"cookie", "date"),
						new ReduceAndKeyFormation(),
						new Fields("newGeo", "newPub", "newBid", "newCookie",
								"newDate", "geopub"))
				.partitionBy(new Fields("geopub"))
				.each(new Fields("newGeo", "newPub", "newBid", "newCookie",
						"newDate", "geopub"), new CollectFor10Secs(),
						new Fields("batch"))
				.parallelismHint(20)
	            .each(new Fields("batch"), new Utils.PartitionPrint())
				.each(new Fields("batch"),
						new CalculateAvgInBatch(),
						new Fields("dateMinute", "geoKey", "pubKey", "bidAvg",
								"unique", "imp"))
				.each(new Fields("dateMinute", "geoKey", "pubKey", "bidAvg",
						"unique", "imp"), new PersistancePreprationBolt(),
						new Fields("savedbatch"))
				.parallelismHint(5)
				.each(new Fields("savedbatch"), new PersistanceBolt(),
						new Fields("-"));

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setNumWorkers(4);
		// LocalDRPC drpc = new LocalDRPC();
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("logAnalysis", conf, buildTopology(null));
		} else {
			StormSubmitter.submitTopology("logAnalysis-trident",conf,buildTopology(null));
		}

	}
}
