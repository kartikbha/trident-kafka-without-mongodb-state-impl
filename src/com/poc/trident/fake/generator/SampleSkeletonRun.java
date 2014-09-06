package com.poc.trident.fake.generator;

import java.io.IOException;

import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;


public class SampleSkeletonRun {

	public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {

		FakeLogGeneratorBatchSpout spout = new FakeLogGeneratorBatchSpout();
		TridentTopology topology = new TridentTopology();
		// id", "geo", "pub", "adv", "website","bid","cookie","date");
		topology.newStream("spout", spout).each(new Fields("id", "geo", "pub", "adv", "website","bid","cookie","date"),
		    new Utils.PrintFilter());

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test-run", conf, buildTopology(drpc));
	}
}
