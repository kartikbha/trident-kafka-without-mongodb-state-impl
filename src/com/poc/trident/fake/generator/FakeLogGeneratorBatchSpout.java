package com.poc.trident.fake.generator;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Random;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
 
/**
 * 
 *  Spout it's generate fake logs pattern using random probability distribution 
 *  for below param
 *  "id", "geo", "pub", "adv", "website","bid","cookie","date";
 *  
 * @author Kartik
 *
 */

@SuppressWarnings({ "serial", "rawtypes" })
public class FakeLogGeneratorBatchSpout implements IBatchSpout {

	private int batchSize;


	public final static String[] GEO = { "NY", "NC", "OH", "PA", "WA", "unknown" };
	public final static String[] PUBLISHER = { "pub1", "pub2", "pub3", "pub4", "pub5","pub6" };
	
	public final static String[] ADVERTISER = { "adv10", "adv20", "adv30", "adv40", "adv50" ,"adv60"};
	
	public final static String[] WEBSITE = { "www.abc.com", "www.qwe.com", "www.xyz.com", "www.123.com", "www.a12.com","www.ed.com" };
	
	public final static String[] BID = { "0.001", "0.002", "0.006", "0.007", "0.005","0.009" };
	
	public final static String[] COOKIE = { "1214", "3453", "3432", "1256", "1267","3435" };
	
	private double[] activityDistribution;
	private Random randomGenerator;

	private long logId = 0;

	public FakeLogGeneratorBatchSpout() throws IOException {
		this(5);
	}

	public FakeLogGeneratorBatchSpout(int batchSize) throws IOException {
		this.batchSize = batchSize;
	}

	@SuppressWarnings("unchecked")
    @Override
	public void open(Map conf, TopologyContext context) {
		// init
		System.err.println("Open Spout instance");
		this.randomGenerator = new Random();
	    // will be on website. 
		this.activityDistribution = getProbabilityDistribution(WEBSITE.length, randomGenerator);
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		
		System.out.println(" batch id "+batchId);
		// emit batchSize fake logs
		for(int i = 0; i < batchSize; i++) {
			collector.emit(getNextLogs());
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}

	@Override
	public void ack(long batchId) {
		// nothing to do here
	}

	@Override
	public void close() {
		// nothing to do here
	}

	@Override
	public Map getComponentConfiguration() {
		// no particular configuration here
		return new Config();
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("id", "geo", "pub", "adv", "website","bid","cookie","date");
	}

	// --- Helper methods --- //
	// SimpleDateFormat is not thread safe!
	private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss aa");

	public static int randInt(int min, int max) {

	    Random rand = new Random();
	    int randomNum = rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	private Values getNextLogs()  {
		
		try {
			Thread.sleep(randInt(1,10)*20);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new Values(++logId + "", GEO[randomIndex(activityDistribution, randomGenerator)], PUBLISHER[randomIndex(activityDistribution, randomGenerator)],ADVERTISER[randomIndex(activityDistribution, randomGenerator)], WEBSITE[randomIndex(activityDistribution, randomGenerator)], BID[randomIndex(activityDistribution, randomGenerator)], COOKIE[randomIndex(activityDistribution, randomGenerator)], DATE_FORMAT.format(System
		    .currentTimeMillis()));
	}

	/**
	 * Code snippet: http://stackoverflow.com/questions/2171074/generating-a-probability-distribution Returns an array of
	 * size "n" with probabilities between 0 and 1 such that sum(array) = 1.
	 */
	private static double[] getProbabilityDistribution(int n, Random randomGenerator) {
		double a[] = new double[n];
		double s = 0.0d;
		for(int i = 0; i < n; i++) {
			a[i] = 1.0d - randomGenerator.nextDouble();
			a[i] = -1 * Math.log(a[i]);
			s += a[i];
		}
		for(int i = 0; i < n; i++) {
			a[i] /= s;
		}
		return a;
	}

	private static int randomIndex(double[] distribution, Random randomGenerator) {
		double rnd = randomGenerator.nextDouble();
		double accum = 0;
		int index = 0;
		for(; index < distribution.length && accum < rnd; index++, accum += distribution[index - 1])
			;
		return index - 1;
	}

	public static void main(String[] args) throws IOException, ParseException {
		FakeLogGeneratorBatchSpout spout = new FakeLogGeneratorBatchSpout();
		spout.open(null, null);
		for(int i = 0; i < 30; i++)
			System.out.println(spout.getNextLogs());
	}
}
