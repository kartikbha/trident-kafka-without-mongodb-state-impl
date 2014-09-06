package com.poc.trident.topology.cluster;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class PersistancePreprationBolt extends BaseFunction {

	private static final Logger LOG = LoggerFactory
			.getLogger(PersistancePreprationBolt.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub

		// "dateMinute","geoKey","pubKey","bidAvg","unique","imp"
		// savedbatch
		Map<String, String> totalRecord = new HashMap<String, String>();
		// date 1
		totalRecord.put("date", (String) tuple.getValueByField("dateMinute"));
		// pub 2
		totalRecord.put("publisher", (String) tuple.getValueByField("pubKey"));
		// geo 3
		totalRecord.put("geo", (String) tuple.getValueByField("geoKey"));
		// avg bid 4
		//new Float(avgBid).toString()
		totalRecord.put("avgBids", new Float((float) tuple.getValueByField("bidAvg")).toString());
		// uniques 6
		totalRecord.put("uniques", new Integer((int) tuple.getValueByField("unique")).toString());
		// imp
		totalRecord.put("imps", new Integer((int) tuple.getValueByField("imp")).toString());

		collector.emit(new Values(totalRecord));

	}
}
