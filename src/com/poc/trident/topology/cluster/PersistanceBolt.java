package com.poc.trident.topology.cluster;

import java.net.UnknownHostException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

public class PersistanceBolt  extends BaseFunction {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(PersistanceBolt.class);
	
	String MONDO_DB_HOST = "localhost";
	String MONDO_DB = "test2";
	String MONDO_DB_COLLECTION = "testcollection2";
	int MONDO_DB_PORT = 27017;
	
	DB db;
    public void prepare() {
	    Mongo mongo = null;
		try {
			mongo = new Mongo(MONDO_DB_HOST, MONDO_DB_PORT);
		} catch (UnknownHostException e) {
			LOG.error("not able to connect  to mongo db "+e);
		}
		db = mongo.getDB(MONDO_DB);
	
	}

    @Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

    	prepare();
    	Map<String,String> recordToSave =  (Map<String, String>) tuple.getValueByField("savedbatch");
		System.out.println("recordToSave      "+recordToSave);
		//LOG.info(" recordToSave "+recordToSave);
		System.out.println("recordToSave size "+recordToSave.size());
		DBCollection collection = db.getCollection(MONDO_DB_COLLECTION);
		
		collection.insert(new BasicDBObject(recordToSave));

		collector.emit(new Values("-"));
	}

	
	
}
