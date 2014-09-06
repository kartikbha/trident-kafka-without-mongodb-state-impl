package com.poc.trident.topology.cluster;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author Kartik
 * 
 */
public class KafkaMessageProcessor extends BaseFunction {
	
	private static JsonFactory factory;
	ObjectMapper mapper = new ObjectMapper(factory);
	

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		/*
		 * 
		List<Object> tupleObjects = (List<Object>) tuple;
		List<List<String>> tupleCollectedFor10Secs = (List<List<String>>) tupleObjects
				.get(0);
     	*/
		//System.out.println(" CollectPrint tuple  " + tuple);
		
		String inputValue = tuple.getString(0).toString();
		ObjectNode obj2 = null;
		try {
			obj2 = mapper.readValue(inputValue,ObjectNode.class);
		} catch (IOException e) {
		}
	
		//System.out.println(" obj2-------> "+obj2);
		
		Values val = new Values();
		String geo = obj2.get("geo").asText();
		val.add(geo);
		String pub = obj2.get("publisher").asText();
		val.add(pub);
        val.add(obj2.get("advertiser").asText());
		val.add(obj2.get("website").asText());
		double bid = obj2.get("bid").asDouble();
		val.add(new Double(bid).toString());
		val.add(obj2.get("cookie").asText());
        String timeStampStr = obj2.get("timestamp").asText();
		//System.out.println(" timeStamp---str------> "+timeStampStr);
		long timeStamp = Long.parseLong(timeStampStr);
		//System.out.println(" timeStamp---------> "+timeStamp);
	    SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss aa");
     
        String fullTime = DATE_FORMAT.format(timeStamp);
        val.add(fullTime);
    
    	System.out.println(" CollectPrint tuple befor emitting...." + val);
    	
        collector.emit(val);
      }

}
