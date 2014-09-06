package com.poc.trident.topology.cluster;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Kartik
 * 
 */
public class ReduceAndKeyFormation extends BaseFunction {

	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		generateTimeUptoMinuteAndEmit(tuple, collector);
	}

	private void generateTimeUptoMinuteAndEmit(TridentTuple tuple,
			TridentCollector collector) {

		Values val = new Values();
		val.add(tuple.getValueByField("geo"));
		val.add(tuple.getValueByField("pub"));
		val.add(tuple.getValueByField("bid"));
		val.add(tuple.getValueByField("cookie"));
		val.add((String) tuple.getValueByField("date"));
		val.add((String)tuple.getValueByField("geo")+(String)tuple.getValueByField("pub"));
		collector.emit(val);
	}
}
