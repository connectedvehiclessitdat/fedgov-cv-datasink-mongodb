package gov.usdot.cv.mongodb.datasink.db;

import gov.usdot.cv.common.database.mongodb.dao.InsertSitDataDao;
import gov.usdot.cv.common.util.PropertyLocator;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;

public class CollectionIndexMonitor implements Runnable {
	private static final int 	DURATION_IN_SECS 			= 0;
	
	private final Logger logger = Logger.getLogger(getClass());

	private InsertSitDataDao 		dao;
	private int 					interval;
	private String					ttlFieldName;
	private Set<String> 			collections;
	private boolean 				terminated = false;
	private String					indexDefinitionList;
	
	public CollectionIndexMonitor() {
		this.interval = PropertyLocator.getInt("mongodb.expiration.index.monitor.interval", 10 * 60 * 1000);
	}
	
	public void setMongoDbDao(InsertSitDataDao dao) {
		this.dao = dao;
	}
	
	public void setCollections(String [] collections) {
		this.collections = new HashSet<String>();
		for (String collection : collections) {
			this.collections.add(collection);
		}
	}
	
	public void setTimeToLiveFieldName(String ttlFieldName) {
		this.ttlFieldName = ttlFieldName;
	}
	
	public void setIndexDefinitionList(String indexDefinitionList) {
		this.indexDefinitionList = indexDefinitionList;
	}

	public void terminate() {
		this.terminated = true;
	}
	
	public void run() {
		logger.info("Collection index monitor [" + Thread.currentThread().getId() + "] is starting ...");
		while (! this.terminated) try {
			logger.info("Analyzing collections ...");
			try {
				analyzeAndUpdate(this.dao.getCollectionNames());
			} catch (Exception ex) {
				logger.error("Failed to analyze and update collections.", ex);
			}
			try { Thread.sleep(this.interval); } catch (InterruptedException ignore) {}
		} catch (Exception ex) {
			logger.error("Failed to run document expiration index monitor.", ex);
		}
		logger.info("Document expiration index monitor [" + Thread.currentThread().getId() + "] is terminated.");
	}
	
	public static BasicDBObject buildIndexObject(String indexString) {
		BasicDBObject dbObj = new BasicDBObject();
		String[] nameValues = indexString.split(" ");
		for (String nameValue: nameValues) {
			String name = nameValue.substring(0, nameValue.indexOf(":"));
			String value = nameValue.substring(nameValue.indexOf(":")+1);
			dbObj.put(name, guessType(value));
		}
		System.out.println(dbObj);
		return dbObj;
	}
	
	public static Object guessType(String value) {
		if (value.startsWith("\"") && value.endsWith("\""))
			return value.substring(1, value.length()-1);
		else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"))
			return Boolean.valueOf(value);
		else if (NumberUtils.isNumber(value))
			return Integer.parseInt(value);
		else
			return value;
	}
	
	private void analyzeAndUpdate(Set<String> collectionNames) {
		if (collectionNames == null || collectionNames.size() == 0) {
			logger.info("No collections found, going back to sleep.");
			return;
		}
		
		for (String collectionName : collectionNames) try {
			if (! collections.contains(collectionName)) {
				logger.debug(String.format("Not analyzing index for collection '%s'.", collectionName));
				continue;
			}
		
			logger.info(String.format("Ensuring index '%s' with expiration %s seconds ...", this.ttlFieldName, DURATION_IN_SECS));
			this.dao.createExpirationIndex(collectionName, this.ttlFieldName, DURATION_IN_SECS);
			
			this.dao.createIndexes(collectionName, this.indexDefinitionList);

		} catch (Exception ex) {
			logger.error("Failed to analyze and update collection '" + collectionName + "'.", ex);
		}
	}
}