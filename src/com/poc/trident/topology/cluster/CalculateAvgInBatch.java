package com.poc.trident.topology.cluster;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Kartik
 * 
 */
public class CalculateAvgInBatch extends BaseFunction {

	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		List<Object> tupleObjects = (List<Object>) tuple;
		List<List<String>> collectedTuples = (List<List<String>>) tupleObjects
				.get(0);
     	// System.out.println(" collectedTuples.get(0).get(4))  date " + collectedTuples.get(0));
		System.out.println(" collectedTuples  size " + collectedTuples.size());
	
		//"newGeo","newPub","newBid","newCookie","newDate","geopub"
		Values val = new Values();
		//date
		val.add(getDateUptoMinute(collectedTuples.get(0).get(4)));
	    //geo
		val.add(collectedTuples.get(0).get(0));
		// pub
		val.add(collectedTuples.get(0).get(1));
	
		float total=0;
		List<String> cookies = new ArrayList<String>();
		for(List<String> collectedTuple : collectedTuples) {
			total = total + Float.parseFloat((String)collectedTuple.get(2));
			collectedTuple.get(2);
			String cookie = (String)collectedTuple.get(3);
			if(!cookies.contains(cookie)){
				cookies.add(cookie);
			}
		}
		float avg = total/(float)collectedTuples.size();
		// bid Avg
		val.add(avg);
		// unique = size of cookies.
		val.add(cookies.size());
		// imps = size of collectedTuples.
		val.add(collectedTuples.size());
		
		System.out.println("before emitting in CalculateAvgInBatch : "+val);
		collector.emit(val);
	
	}
	
	private String getDateUptoMinute(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		String dateUptoMinute = null;
		try {
			dateUptoMinute = df.format(df.parse(field));
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}

	
	
}
