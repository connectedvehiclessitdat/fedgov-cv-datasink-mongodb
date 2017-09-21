package gov.usdot.cv.mongodb.datasink;

import gov.usdot.cv.common.database.mongodb.MongoOptionsBuilder;
import gov.usdot.cv.common.database.mongodb.dao.InsertSitDataDao;
import gov.usdot.cv.common.dialog.Receipt;
import gov.usdot.cv.common.dialog.ReceiptSender;
import gov.usdot.cv.common.util.InstanceMetadataUtil;
import gov.usdot.cv.common.util.PropertyLocator;
import gov.usdot.cv.mongodb.datasink.db.CollectionIndexMonitor;
import gov.usdot.cv.mongodb.datasink.model.DataModel;

import java.util.UUID;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.exception.InitializationException;
import com.deleidos.rtws.core.framework.Description;
import com.deleidos.rtws.core.framework.SystemConfigured;
import com.deleidos.rtws.core.framework.UserConfigured;
import com.deleidos.rtws.core.framework.processor.AbstractDataSink;
import com.mongodb.BasicDBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

@Description("Stores traveler information data into a Mongo database.")
public class MongoDbDataSink extends AbstractDataSink {
	
	private static final Object LOCK = new Object();
	private static final String TTL_UNITS = "^(minute|day|week|month|year)$";
	private static final String ENCODED_MSG = "encodedMsg";
	
	private final Logger logger = Logger.getLogger(getClass());
	
	private String 		databaseName;
	private String 		mongoServerHost;
	private int    		mongoServerPort;
	private boolean 	autoConnectRetry = true;
	private int 		connectTimeoutMs = 0;
	private boolean		ignoreMessageTTL = false;
	private String 		ttlUnit;
	private int 		ttlValue;
	private String 		topicName;
	private String 		ttlFieldName = "expireAt";
	private String		indexDefinitionList;
	private String [] 	collections;
	
	private static CollectionIndexMonitor monitor;
	private static Thread monitor_t;
	
	private InsertSitDataDao 	dao;
	private ReceiptSender 		sender;
	
	@Override
	@SystemConfigured(value = "MongoDB Data Sink")
	public void setName(String name) {
		super.setName(name);
	}

	@Override
	@SystemConfigured(value = "mongo")
	public void setShortname(String shortname) {
		super.setShortname(shortname);
	}
	
	@UserConfigured(value="cvdb", description="The database name to use to store processed records.")
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDatabaseName() {
		return this.databaseName;
	}
	
	@UserConfigured(
		value= "mongodb.%s", 
		description="The MongoDB server hostname.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setMongoServerHost(String mongoServerHost) {
		this.mongoServerHost = mongoServerHost;
	}
	
	@NotNull
	public String getMongoServerHost() {
		return this.mongoServerHost;
	}
	
	@UserConfigured(
		value = "27017", 
		description = "The MongoDB server port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setMongoServerPort(int mongoServerPort) {
		this.mongoServerPort = mongoServerPort;
	}
	
	@Min(0)
	@Max(65535)
	public int getMongoServerPort() {
		return this.mongoServerPort;
	}

	@UserConfigured(
		value = "true",
		description = "MongoDB client auto connect retry flag.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}
	
	@UserConfigured(
		value = "3000",
		description = "Time (in milliseconds) to wait for a successful connection.",
		flexValidator = {"NumberValidator minValue=0 maxValue=" + Integer.MAX_VALUE})
	public void setConnectTimeoutMs(int connectTimeoutMs) {
		this.connectTimeoutMs = connectTimeoutMs;
	}
	
	@UserConfigured(
		value= "false",
		flexValidator = { "RegExpValidator expression=^(true|false)$" },
		description="Flag indicating if we uses the message ttl or the configured ttl.")
	public void setIgnoreMessageTimeToLive(String ignoreMessageTTL) {
		this.ignoreMessageTTL = Boolean.valueOf(ignoreMessageTTL);
	}
	
	@UserConfigured(
		value = "30",
		description = "The document's time to live value.",
		flexValidator = {"NumberValidator minValue=0 maxValue=" + Integer.MAX_VALUE})
	public void setTimeToLiveValue(int ttlValue) {
		this.ttlValue = ttlValue;
	}
	
	@UserConfigured(
		value= "minute",
		flexValidator = { "RegExpValidator expression=" + TTL_UNITS },
		description="The unit of the time of live value.")
	public void setTimeToLiveUnit(String ttlUnit) {
		this.ttlUnit = ttlUnit;
	}
	
	@UserConfigured(
		value = "expireAt",
		description = "The field to create a MongoDB expiration index.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setTimeToLiveFieldName(String ttlFieldName) {
		this.ttlFieldName = ttlFieldName;
	}
	
	@UserConfigured(
		value = "region:2dsphere createdAt:1, requestId:1 createdAt:1, createdAt:1",
		description = "Defines the indexes to create on each collection.  Indexes are defined as <fieldName>:<value> " +
				"and separated by a comma.  Use a space between indexes for compound indexes.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setIndexDefinitionList(String indexDefinitionList) {
		this.indexDefinitionList = indexDefinitionList;
	}
	
	@UserConfigured(
		value = "cv.receipts",
		description = "The external jms topic to place receipt.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	@UserConfigured(
		value = "vehSitDataMessage,travelerInformation,intersectionSitData",
		description = "List of input data model names, delimited by comma, used to determine which collections to create indexes.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setCollections(String collections) {
		if (StringUtils.isNotBlank(collections)) {
			this.collections = collections.trim().split("[,]");
		}
	}

	public void initialize() throws InitializationException {
		try {
			logger.info("Constructing MongoDB data access object ...");
			MongoOptionsBuilder optionsBuilder = new MongoOptionsBuilder();
			optionsBuilder.setAutoConnectRetry(this.autoConnectRetry).setConnectTimeoutMs(this.connectTimeoutMs);
			MongoOptions options = optionsBuilder.build();
			
			String domain = PropertyLocator.getString("RTWS_DOMAIN", null);
			this.mongoServerHost = (domain == null) ? "localhost" : String.format(mongoServerHost, domain);
			
			logger.info("Testing MongoDB connection ...");
			testConnectivity(options);
			
			logger.info(String.format("Setting MongoDB host to '%s' and port to '%s'.", this.mongoServerHost, this.mongoServerPort));
			this.dao = InsertSitDataDao.newInstance(
				this.mongoServerHost, 
				this.mongoServerPort, 
				options,
				this.databaseName);
			
			logger.info("Constructing receipt sender ...");
			String brokerUrl = PropertyLocator.getString("messaging.external.connection.url");
			if (brokerUrl == null) {
				throw new InitializationException("Missing property 'messaging.external.connection.url'.");
			}
			
			String username = PropertyLocator.getString("messaging.external.connection.user");
			if (username == null) {
				throw new InitializationException("Missing property 'messaging.external.connection.user'.");
			}
			
			String password = PropertyLocator.getString("messaging.external.connection.password");
			if (password == null) {
				throw new InitializationException("Missing property 'messaging.external.connection.password'.");
			}
			
			ReceiptSender.Builder senderBuilder = new ReceiptSender.Builder();
			senderBuilder.setBrokerUrl(brokerUrl).setUsername(username)
				.setPassword(password).setTopicName(this.topicName);
			this.sender = senderBuilder.build();

			synchronized (LOCK) {
				// The first mongoDB datasink node and the first thread creates
				// the index monitor thread and receipt topic.
				if (monitor_t == null && InstanceMetadataUtil.getNodeNumber() == 1) {
					logger.info("Initializing collection index monitor ...");
					monitor = new CollectionIndexMonitor();
					monitor.setMongoDbDao(this.dao);
					monitor.setCollections(this.collections);
					monitor.setTimeToLiveFieldName(this.ttlFieldName);
					monitor.setIndexDefinitionList(this.indexDefinitionList);
					monitor_t = new Thread(monitor);
					monitor_t.start();
					
					logger.info(String.format("Creating receipt topic '%s' on external jms server ...", this.topicName));
					Receipt.Builder builder = new Receipt.Builder();
					builder.setReceiptId(UUID.randomUUID().toString());
					this.sender.send(builder.build().toString());
				}
			}
		} catch (Exception ex) {
			throw new InitializationException("Failed to initialize MongoDbDataSink.", ex);
		}
	}

	public void dispose() {
		if (this.sender != null) {
			this.sender.close();
			this.sender = null;
		}
		
		synchronized (LOCK) {
			if (monitor != null && monitor_t != null) {
				monitor.terminate();
				try { monitor_t.join(5000); } catch (InterruptedException e) {}
				monitor = null;
				monitor_t = null;
			}
		}
	}

	@Override
	protected void processInternal(JSONObject record, FlushCounter counter) {
		try {
			if (storeRecord(record)) {
				sendReceipt(record);
			}
		} catch (Exception ex) {
			logger.error("Failed to process advisory situation data record.", ex);
		}
	}
	
	public void flush() {
		logger.debug(String.format("The method flush() is not used by this class '%s'.", this.getClass().getName()));
	}
	
	private void testConnectivity(MongoOptions options) {
		while (true) try {
			Mongo client = new Mongo(new ServerAddress(this.mongoServerHost, this.mongoServerPort));
			client.getDatabaseNames();
			return;
		} catch (Exception ex) {
			logger.error(String.format("Failed to connect to MongoDB at '%s:%s'", 
				this.mongoServerHost, this.mongoServerPort), ex);
		}
	}
	
	private boolean storeRecord(JSONObject record) throws Exception {
		int retries = 3;
		while (retries >= 0) {
			try {
				DataModel model = new DataModel(
					record,
					this.ttlFieldName, 
					this.ignoreMessageTTL,
					this.ttlValue, 
					this.ttlUnit);
				
				BasicDBObject query = model.getQuery();
				BasicDBObject doc = model.getDoc();
				
				if (!doc.containsField(ENCODED_MSG)) {
					logger.error("Missing " + ENCODED_MSG + " in record " + record);
					return false;
				}
				
				WriteResult result = null;
				if (model.getQuery() != null) {
					result = this.dao.upsert(model.getModelName(), query, doc);
				} else {
					result = this.dao.insert(model.getModelName(), doc);
				}
				
				return true;
				
			} catch (Exception ex) {
				logger.error(String.format("Failed to store record into MongoDB. Message: %s", ex.getMessage()), ex);
			} finally {
				retries--;
			}
			
			try { Thread.sleep(10); } catch (Exception ignore) {}
		}
		
		logger.error("Failed to store record into MongoDB, retries exhausted. Record: " + record.toString());
		
		return false;
	}
	
	private void sendReceipt(JSONObject record) {
		try {
			if (record.has("receiptId")) {
				Receipt.Builder builder = new Receipt.Builder();
				builder.setReceiptId(record.getString("receiptId"));
				this.sender.send(builder.build().toString());
			} else {
				logger.debug("Receipt not sent because 'receiptId' doesn't exist. Record: " + record.toString());
			}
		} catch (Exception ex) {
			logger.error("Failed to send receipt to external jms server.", ex);
		}
	}
}