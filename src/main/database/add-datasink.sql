-- Run the following statements to make System Builder UI tool aware of the datasink.
INSERT INTO APPLICATION.PROCESS_GROUP_CONFIG VALUES(
	'datasink.cvmongodb', 
	'datasink.default', 
	'private', 
	'cv-mongodb?.@build.domain@', 
	'ingest.rtws.saic.com', 
	null, 
	'm1.large', 
	'instance', 
	null, 
	'datasink-mongo.ini', 
	'services.mongo.xml', 
	'{"default-num-volumes" : 0, "default-volume-size" : 0, "config-volume-size" : false, "config-persistent-ip" : false, "default-num-instances" : 1, "config-instance-size" : true, "config-min-max" : true, "config-scaling" : true, "config-jms-persistence" : false }'
);
	
INSERT INTO APPLICATION.DATASINK_CONFIG VALUES(
	'gov.usdot.cv.mongodb.datasink.MongoDbDataSink',
	'Y',
	'N',
	0.75,
	'',
	'',
	'',
	'mongodb.standalone, datasink.cvmongodb'
);	