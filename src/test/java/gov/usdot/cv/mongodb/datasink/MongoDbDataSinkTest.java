package gov.usdot.cv.mongodb.datasink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import gov.usdot.cv.common.database.mongodb.MongoClientBuilder;
import gov.usdot.cv.common.database.mongodb.geospatial.Coordinates;
import gov.usdot.cv.common.database.mongodb.geospatial.Geometry;
import gov.usdot.cv.common.database.mongodb.geospatial.Point;
import gov.usdot.cv.common.util.UnitTestHelper;
import gov.usdot.cv.resources.PrivateTestResourceLoader;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import net.sf.json.JSONObject;

import org.apache.activemq.broker.BrokerService;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;

/**
 * MongoDB doesn't offer a embedded mode function, so these unit tests were written to uses a
 * MongoDB instance running on your localhost.
 */
public class MongoDbDataSinkTest {
	
	static
	{
		UnitTestHelper.initLog4j(Level.INFO);
		Properties testProperties = System.getProperties();
		if (testProperties.getProperty("RTWS_CONFIG_DIR") == null) {
			try {
				testProperties.load(PrivateTestResourceLoader.getFileAsStream("@properties/datasink-mongodb-filtering.properties@"));

				System.setProperties(testProperties);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static final String REMOTE_JMS_BROKER_NAME = "EXTERNAL_JMS_BROKER";
	private static final String REMOTE_JMS_CONNECTOR_URL = "tcp://localhost:61619";
	
	private static BrokerService externalJMSBroker;
	
	private static final String BASE64_VEH_SIT_DATA = "MHCAAgCKgQEBokWgKaATgAIH3oEBAoIBCoMBCIQBHoUBGYEBK4IBVYMCA0iEAQCFAgA3hwEkgQEAggECoxKDEP/u/rQAAABk//n+yAAAAGSDIAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
	private static final String BASE64_ADV_SIT_DATA = "MFaAAgCcgQEFggQAAAPpoxygDIAEGaFHgIEEzVYHgKEMgAQYcBqAgQTPH8sApCmABAAAFbOBAQKCAgeAhRpFeGl0IDYyIGNsb3NlZCwgdXNlIGRldG91cg==";
	
	private static JSONObject vehsitdata1;
	private static JSONObject vehsitdata2;
	private static JSONObject advsitdata1;
	private static JSONObject advsitdata2_badregion;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.out.println("Building test dataset ...");
		buildTestData();
		
		System.out.println("Setting 'RTWS_FQDN' and 'RTWS_DOMAIN' properties ...");
		System.setProperty("RTWS_FQDN", "mongodb-writer-node1.dev-rhsdw.cv-dev.aws-dev.deleidos.com");
		System.setProperty("RTWS_DOMAIN", "dev-rhsdw.cv-dev.aws-dev.deleidos.com");
		System.setProperty("messaging.external.connection.url", "nio://localhost:61619");
		
		System.out.println("Starting external jms broker service ...");
		externalJMSBroker = new BrokerService();
		externalJMSBroker.setBrokerName(REMOTE_JMS_BROKER_NAME);
		externalJMSBroker.addConnector(REMOTE_JMS_CONNECTOR_URL);
		externalJMSBroker.setPersistent(false);
		externalJMSBroker.setUseJmx(false);
		externalJMSBroker.start();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		System.out.println("Stopping external jms broker service ...");
		externalJMSBroker.stop();
	}
	
	@Test @Ignore
	public void testInitializeMongoDbDatasink() {
		System.out.println(">>> Running testInitializeMongoDbDatasink() ...");
		
		try {
			MongoDbDataSink sink = new MongoDbDataSink();
			sink.setDatabaseName("cvdb");
			sink.setMongoServerHost("localhost");
			sink.setMongoServerPort(27017);
			sink.setAutoConnectRetry(true);
			sink.setConnectTimeoutMs(3000);
			sink.setTimeToLiveValue(30);
			sink.setTimeToLiveUnit("minute");
			sink.setTimeToLiveFieldName("createdAt");
			sink.setTopicName("cv.receipts");
			sink.setCollections("vehSitDataMessage,travelerInformation");
			sink.initialize();
			sink.dispose();
		} catch (Exception ex) {
			Assert.fail("Failed to initialize MongoDB datasink. Message: " + ex.getMessage());
		}
	}
	
	@Test @Ignore
	public void testStoreVehicleSitDataRecord() throws Exception {
		System.out.println(">>> Running testStoreVehicleSitDataRecord() ...");
		
		String dbname = "cvdb";
		String host = "localhost";
		int port = 27017;
		boolean autoRetry = true;
		int timeout = 3000;
		
		MongoDbDataSink sink = new MongoDbDataSink();
		sink.setDatabaseName(dbname);
		sink.setMongoServerHost(host);
		sink.setMongoServerPort(port);
		sink.setAutoConnectRetry(autoRetry);
		sink.setConnectTimeoutMs(timeout);
		sink.setTimeToLiveValue(30);
		sink.setTimeToLiveUnit("minute");
		sink.setTimeToLiveFieldName("createdAt");
		sink.setTopicName("cv.receipts");
		sink.setCollections("vehSitDataMessage,travelerInformation");
		sink.initialize();
		sink.process(vehsitdata1);
		
		MongoOptions options = new MongoOptions();
		//options.autoConnectRetry = autoRetry;
		options.connectTimeout = timeout;
		
		Mongo client = buildMongo(host, port, options);
		DB database = client.getDB(dbname);
		DBCollection collection = database.getCollection("vehSitDataMessage");
		
		assertNotNull("Collection vehSitDataMessage doesn't exist.", collection);
		
		BasicDBObject query = new BasicDBObject();
		query.put("standardHeader.uuid", "88aec29c-99ed-4199-ab8c-a7c681c7a9d1");
		
		DBObject result = collection.findOne(query);
		
		assertNotNull("Did not find document with uuid '88aec29c-99ed-4199-ab8c-a7c681c7a9d1'", result);
		assertEquals("Document returned a encoded message that didn't match what was inserted.", BASE64_VEH_SIT_DATA, result.get("encodedMsg"));
		
		sink.dispose();
	}
	
	@Test @Ignore
	public void testVehicleSitDataRecordExpiration() throws Exception {
		System.out.println(">>> Running testVehicleSitDataRecordExpiration() ...");
		
		String dbname = "cvdb";
		String host = "localhost";
		int port = 27017;
		boolean autoRetry = true;
		int timeout = 3000;
		
		MongoDbDataSink sink = new MongoDbDataSink();
		sink.setDatabaseName(dbname);
		sink.setMongoServerHost(host);
		sink.setMongoServerPort(port);
		sink.setAutoConnectRetry(autoRetry);
		sink.setConnectTimeoutMs(timeout);
		sink.setTimeToLiveValue(30);
		sink.setTimeToLiveUnit("minute");
		sink.setTimeToLiveFieldName("createdAt");
		sink.setTopicName("cv.receipts");
		sink.setCollections("vehSitDataMessage,travelerInformation");
		sink.initialize();
		sink.process(vehsitdata2);
		
		MongoOptions options = new MongoOptions();
		//options.autoConnectRetry = autoRetry;
		options.connectTimeout = timeout;
		
		Mongo client = buildMongo(host, port, options);
		DB database = client.getDB(dbname);
		DBCollection collection = database.getCollection("vehSitDataMessage");
		
		assertNotNull("Collection vehSitDataMessage doesn't exist.", collection);
		
		BasicDBObject query1 = new BasicDBObject();
		query1.put("standardHeader.uuid", "99aec29c-99ed-4199-ab8c-a7c681c7a9d1");
		
		DBObject result1 = collection.findOne(query1);
		
		assertNotNull("Did not find document with uuid '99aec29c-99ed-4199-ab8c-a7c681c7a9d1'.", result1);
		assertEquals("Document returned a encoded message that didn't what was inserted.", BASE64_VEH_SIT_DATA, result1.get("encodedMsg"));

		// wait for the record to expire from the database
		try { Thread.sleep(1000 * 60); } catch (InterruptedException ie) {}
		
		BasicDBObject query2 = new BasicDBObject();
		query2.put("standardHeader.uuid", "99aec29c-99ed-4199-ab8c-a7c681c7a9d1");
		
		DBObject result2 = collection.findOne(query2);
		
		assertNull("Found document with uuid '99aec29c-99ed-4199-ab8c-a7c681c7a9d1', it should have been expired.", result2);
		
		sink.dispose();
	}
	
	@Test @Ignore
	public void testStoreAdvSitDataRecord() throws Exception {
		System.out.println(">>> Running testStoreAdvSitDataRecord() ...");
		
		String dbname = "cvdb";
		String host = "localhost";
		int port = 27017;
		boolean autoRetry = true;
		int timeout = 3000;
		
		MongoDbDataSink sink = new MongoDbDataSink();
		sink.setDatabaseName(dbname);
		sink.setMongoServerHost(host);
		sink.setMongoServerPort(port);
		sink.setAutoConnectRetry(autoRetry);
		sink.setConnectTimeoutMs(timeout);
		sink.setTimeToLiveValue(30);
		sink.setTimeToLiveUnit("minute");
		sink.setTimeToLiveFieldName("createdAt");
		sink.setTopicName("cv.receipts");
		sink.setIndexDefinitionList("region:2dsphere createdAt:1, requestId:1 createdAt:1, createdAt:1");
		sink.setCollections("vehSitDataMessage,travelerInformation");
		sink.initialize();
		sink.process(advsitdata1);
		
		MongoOptions options = new MongoOptions();
		//options.autoConnectRetry = autoRetry;
		options.connectTimeout = timeout;
		
		Mongo client = buildMongo(host, port, options);
		DB database = client.getDB(dbname);
		DBCollection collection = database.getCollection("travelerInformation");
		
		assertNotNull("Collection travelerInformation doesn't exist.", collection);
		
		BasicDBObject query = new BasicDBObject();
		query.put("standardHeader.uuid", "11aec29c-99ed-4199-ab8c-a7c681c7a9d1");
		
		DBObject result = collection.findOne(query);
		
		assertNotNull("Did not find document with uuid '11aec29c-99ed-4199-ab8c-a7c681c7a9d1'", result);
		assertEquals("Document returned a encoded message that didn't match what was inserted.", BASE64_ADV_SIT_DATA, result.get("encodedMsg"));
		
		sink.dispose();
	}
	
	@Test @Ignore
	public void testInvalidServiceRegionAdvSitDataRecord() throws Exception {
		System.out.println(">>> Running testStoreAdvSitDataRecord() ...");
		
		String dbname = "cvdb";
		String host = "localhost";
		int port = 27017;
		boolean autoRetry = true;
		int timeout = 3000;
		
		MongoDbDataSink sink = new MongoDbDataSink();
		sink.setDatabaseName(dbname);
		sink.setMongoServerHost(host);
		sink.setMongoServerPort(port);
		sink.setAutoConnectRetry(autoRetry);
		sink.setConnectTimeoutMs(timeout);
		sink.setTimeToLiveValue(30);
		sink.setTimeToLiveUnit("minute");
		sink.setTimeToLiveFieldName("createdAt");
		sink.setTopicName("cv.receipts");
		sink.setIndexDefinitionList("region:2dsphere createdAt:1, requestId:1 createdAt:1, createdAt:1");
		sink.setCollections("vehSitDataMessage,travelerInformation");
		sink.initialize();
		sink.process(advsitdata2_badregion);
		
		MongoOptions options = new MongoOptions();
		//options.autoConnectRetry = autoRetry;
		options.connectTimeout = timeout;
		
		Mongo client = buildMongo(host, port, options);
		DB database = client.getDB(dbname);
		DBCollection collection = database.getCollection("travelerInformation");
		
		assertNotNull("Collection travelerInformation doesn't exist.", collection);
		
		BasicDBObject query = new BasicDBObject();
		query.put("standardHeader.uuid", "22aec29c-99ed-4199-ab8c-a7c681c7a9d1");
		
		DBObject result = collection.findOne(query);
		
		assertNull("Found document with uuid '22aec29c-99ed-4199-ab8c-a7c681c7a9d1', should not exist.", result);

		sink.dispose();
	}
	
	private static void buildTestData() {
		StringBuilder sb1 = new StringBuilder();
		sb1.append('{');
			sb1.append(buildStandardHeader("88aec29c-99ed-4199-ab8c-a7c681c7a9d1", "vehSitDataMessage", "1.0"));
			sb1.append(buildCommonVehSitDataModelBody());
		sb1.append('}');
		vehsitdata1 = JSONObject.fromObject(sb1.toString());
		
		StringBuilder sb2 = new StringBuilder();
		sb2.append('{');
			sb2.append(buildStandardHeader("99aec29c-99ed-4199-ab8c-a7c681c7a9d1", "vehSitDataMessage", "1.0"));
			sb2.append(buildCommonVehSitDataModelBody());
		sb2.append('}');
		vehsitdata2 = JSONObject.fromObject(sb2.toString());
		
		StringBuilder sb3 = new StringBuilder();
		sb3.append('{');
			sb3.append(buildStandardHeader("11aec29c-99ed-4199-ab8c-a7c681c7a9d1", "travelerInformation", "1.2"));
			sb3.append(buildCommonAdvSitDataModelBody());
			sb3.append(buildServiceRegion(43, -85, 41, -82));
			sb3.append(buildGeoJson("Polygon", 43, -85, 41, -82));
			sb3.append(buildCommonBroadcastInstructions());
		sb3.append('}');
		advsitdata1 = JSONObject.fromObject(sb3.toString());
		
		StringBuilder sb4 = new StringBuilder();
		sb4.append('{');
			sb4.append(buildStandardHeader("22aec29c-99ed-4199-ab8c-a7c681c7a9d1", "travelerInformation", "1.2"));
			sb4.append(buildCommonAdvSitDataModelBody());
			sb4.append(buildServiceRegion(43, -85, 43, -85));
			sb4.append(buildGeoJson("Polygon", 43, -85, 43, -85));
			sb4.append(buildCommonBroadcastInstructions());
			sb4.append('}');
		advsitdata2_badregion = JSONObject.fromObject(sb4.toString());
	}
	
	private static String buildStandardHeader(String uuid, String modelName, String modelVersion) {
		StringBuilder sb = new StringBuilder();
		sb.append("\"standardHeader\": {");
			sb.append("\"uuid\": \"" + uuid + "\",");
			sb.append("\"source\": \"UNKNOWN\",");
			sb.append("\"accessLabel\": \"UNCLASSIFIED\",");
			sb.append("\"modelName\": \"" + modelName + "\",");
			sb.append("\"modelVersion\": \"" + modelVersion +"\"");
		sb.append("},");
		return sb.toString();
	}
	
	private static String buildCommonVehSitDataModelBody() {
		StringBuilder sb = new StringBuilder();
		sb.append("\"dialogId\": 155,"); 
		sb.append("\"sequenceId\": 5,"); 
		sb.append("\"vsmType\": 1,"); 
		sb.append("\"year\": 2014,");
		sb.append("\"month\": 2,");
		sb.append("\"day\": 10,");
		sb.append("\"hour\": 8,");
		sb.append("\"minute\": 30,");
		sb.append("\"second\": 25,");
		sb.append("\"lat\": 42.44783187,");
		sb.append("\"long\": -83.43090838,");
		sb.append("\"encodedMsg\":\"" + BASE64_VEH_SIT_DATA + "\"");
		return sb.toString();
	}
	
	private static String buildCommonAdvSitDataModelBody() {
		StringBuilder sb = new StringBuilder();
		sb.append("\"dialogId\": 155,");
		sb.append("\"receiptId\": \"91dc722f-5877-4348-a68a-e0dc4fa7c6b6\","); 
		sb.append("\"sequenceId\": 5,");
		sb.append("\"requestId\": 1001,");
		sb.append("\"encodedMsg\":\"" + BASE64_ADV_SIT_DATA + "\",");
		return sb.toString();
	}
	
	private static String buildServiceRegion(double nwLat, double nwLon, double seLat, double seLon) {
		StringBuilder sb = new StringBuilder();
		
		String nwPos = buildPosition(nwLat, nwLon);
		String sePos = buildPosition(seLat, seLon);
		
		sb.append("\"nwPos\": " + nwPos + ",");
		sb.append("\"sePos\": " + sePos);
		sb.append(',');
		
		return sb.toString();
	}
	
	private static String buildPosition(double lat, double lon) {
		JSONObject pos = new JSONObject();
		pos.put("lat", lat);
		pos.put("lon", lon);
		return pos.toString();
	}
	
	private static String buildGeoJson(String type, double nwLat, double nwLon, double seLat, double seLon) {
		Point nwCorner = new Point.Builder().setLat(nwLat).setLon(nwLon).build();
		Point neCorner = new Point.Builder().setLat(nwLat).setLon(seLon).build();
		Point seCorner = new Point.Builder().setLat(seLat).setLon(seLon).build();
		Point swCorner = new Point.Builder().setLat(seLat).setLon(nwLon).build();
		
		Coordinates coordinates = new Coordinates.Builder()
			.addPoint(nwCorner)
			.addPoint(neCorner)
			.addPoint(seCorner)
			.addPoint(swCorner)
			.addPoint(nwCorner)
		.build();
		
		Geometry geometry = new Geometry.Builder().setType(type).setCoordinates(coordinates).build();
		
		StringBuilder sb = new StringBuilder();
		sb.append("\"region\": " + geometry.toJSONObject().toString() + ",");
		return sb.toString();
	}
	
	private static String buildCommonBroadcastInstructions() {
		StringBuilder sb = new StringBuilder();
		sb.append("\"broadcastInstructions\": {");
			sb.append("\"type\": 2,"); 
			sb.append("\"psid\": 32771,"); 
			sb.append("\"priority\": 2,");
			sb.append("\"txMode\": 1,");
			sb.append("\"txInterval\": 5,");
			sb.append("\"deliveryStart\": \"2014-05-08T14:33:30\",");
			sb.append("\"deliveryEnd\": \"2014-05-15T14:33:30\",");
			sb.append("\"signature\": true,");
			sb.append("\"encryption\": false");
		sb.append("}");
		return sb.toString();
	}
	
	private Mongo buildMongo(String host, int port, MongoOptions options) throws UnknownHostException {
		MongoClientBuilder builder = new MongoClientBuilder();
		builder.setHost(host).setPort(port).setMongoOptions(options);
		return builder.build();
	}
	
}