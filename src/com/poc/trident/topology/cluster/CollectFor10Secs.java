package com.poc.trident.topology.cluster;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author Kartik
 * 
 */
public class CollectFor10Secs extends BaseFunction {

	Map<String, List<List<String>>> geopubToListMap = new HashMap<String, List<List<String>>>();
	Map<String, String> startTimeTogeopub = new HashMap<String, String>();

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		collectAndMakeBatch(tuple, collector);
	}

	private Date getFullTimeByDate(String field) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");
		Date dateUptoMinute = null;
		try {
			dateUptoMinute = df.parse(field);
		} catch (ParseException e) {
		}
		return dateUptoMinute;
	}

	private String currentTime() {

		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
				"yyyy-MM-dd hh:mm:ss aa");
		String currentTime = DATE_FORMAT.format(System.currentTimeMillis());
		return currentTime;
	}

	private void collectAndMakeBatch(TridentTuple tuple,
			TridentCollector collector) {

		// System.out.println(" collectAndMakeBatch tuple "+tuple);
		String date = (String) tuple.getValueByField("newDate");
		String geopub = (String) tuple.getValueByField("geopub");
		// System.out.println(" geopub "+geopub);
		if (!startTimeTogeopub.containsKey(geopub)) {
			startTimeTogeopub.put(geopub, date);
		}
		// System.out.println(" startTimeTogeopub "+startTimeTogeopub);
		long diffSeconds = (getFullTimeByDate(currentTime()).getTime() - getFullTimeByDate(
				startTimeTogeopub.get(geopub)).getTime()) / 1000 % 60;
		// System.out.println(" diffSeconds "+diffSeconds+" for geopub "+geopub);
		if (diffSeconds > 10) {
			System.out
					.println(" before emitting geopubToListMap.get(geopub) --> "
							+ geopubToListMap.get(geopub));
			if (geopubToListMap.get(geopub) != null) {
				collector.emit(new Values(geopubToListMap.get(geopub)));
				geopubToListMap.remove(geopub);
				startTimeTogeopub.remove(geopub);
			}
		} else {
			List<List<String>> listOfTuplesFromSameGeoPub = null;
			if (geopubToListMap.containsKey(geopub)) {
				listOfTuplesFromSameGeoPub = geopubToListMap.get(geopub);
				// System.out.println(" adding value in same list before adding "+listOfTuplesFromSameGeoPub);
			} else {
				listOfTuplesFromSameGeoPub = new ArrayList<List<String>>(1000);
				geopubToListMap.put(geopub, listOfTuplesFromSameGeoPub);
			}
			listOfTuplesFromSameGeoPub.add(addTupleIntoList(tuple));
			geopubToListMap.put(geopub, listOfTuplesFromSameGeoPub);
			// System.out.println(" adding value in same list after adding "+geopubToListMap.get(geopub));
		}
	}

	private List<String> addTupleIntoList(TridentTuple tuple) {
		List<String> val = new ArrayList<String>();
		for (int i = 0; i < tuple.size(); i++) {
			val.add((String) tuple.get(i));
		}
		return val;
	}
}
