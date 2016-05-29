package com.echat.storm.analysis;


import storm.kafka.ZkHosts;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.OpaqueTridentKafkaSpout;


import storm.trident.TridentTopology;
import storm.trident.TridentState;
import storm.trident.Stream;
import storm.trident.fluent.GroupedStream;

////
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.generated.StormTopology;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.TopologyBuilder;
//import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;


import org.apache.storm.redis.common.config.JedisPoolConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.operation.*;
import com.echat.storm.analysis.state.*;

public class AnalysisTopology {
	private static final Logger log = LoggerFactory.getLogger(AnalysisTopology.class);

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(
				new ZkHosts(AnalysisTopologyConstranst.ZOOKEEPER_HOST_LIST), 
				AnalysisTopologyConstranst.KAFKA_TOPIC,
				AnalysisTopologyConstranst.ZOOKEEPER_PTTSVC_LOG_SPOUT_ID
				);
		spoutConf.scheme = new SchemeAsMultiScheme(new PttsvcLogInfoScheme());
		spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime(); 
		spoutConf.ignoreZkOffsets = true;

		Stream logStream = topology.newStream(AnalysisTopologyConstranst.SPOUT_INPUT,new OpaqueTridentKafkaSpout(spoutConf)).partitionBy(new Fields(FieldsConstrants.APP_FIELD)).parallelismHint(AnalysisTopologyConstranst.SPORT_INPUT_EXECUTORS); 

		Stream fatalStream = logStream.each(new Fields(FieldsConstrants.LEVEL_FIELD),new LevelFilter("FATAL")).name(AnalysisTopologyConstranst.STREAM_FATAL);
		Stream errorStream = logStream.each(new Fields(FieldsConstrants.LEVEL_FIELD),new LevelFilter("ERROR")).name(AnalysisTopologyConstranst.STREAM_ERROR);
		Stream warnStream = logStream.each(new Fields(FieldsConstrants.LEVEL_FIELD),new LevelFilter("WARNING")).name(AnalysisTopologyConstranst.STREAM_WARN);
		Stream infoStream = logStream.each(new Fields(FieldsConstrants.LEVEL_FIELD),new LevelFilter("INFO")).name(AnalysisTopologyConstranst.STREAM_INFO);

		// event streams
		Stream eventStream = infoStream.each(new Fields(FieldsConstrants.CONTENT_FIELD),new GetEvent(),new Fields(FieldsConstrants.EVENT_FIELD));

		Stream onlineStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(new String[]{
					AnalysisTopologyConstranst.EVENT_LOGIN,
					AnalysisTopologyConstranst.EVENT_RELOGIN,
					AnalysisTopologyConstranst.EVENT_BROKEN,
					AnalysisTopologyConstranst.EVENT_LOGOUT}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseOnlineEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.CTX_FIELD,
					FieldsConstrants.IP_FIELD,
					FieldsConstrants.DEVICE_FIELD,
					FieldsConstrants.DEVICE_ID_FIELD,
					FieldsConstrants.VERSION_FIELD,
					FieldsConstrants.IMSI_FIELD,
					FieldsConstrants.EXPECT_PAYLOAD_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_ONLINE);

		Stream loginFailedStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(AnalysisTopologyConstranst.EVENT_LOGIN_FAILED))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseLoginFailedEvents(),
				new Fields(
					FieldsConstrants.REASON_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.CTX_FIELD,
					FieldsConstrants.IP_FIELD,
					FieldsConstrants.DEVICE_FIELD,
					FieldsConstrants.DEVICE_ID_FIELD,
					FieldsConstrants.VERSION_FIELD,
					FieldsConstrants.IMSI_FIELD,
					FieldsConstrants.EXPECT_PAYLOAD_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_LOGIN_FAILED);
			
		Stream speakStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(new String[]{
					AnalysisTopologyConstranst.EVENT_GET_MIC,
					AnalysisTopologyConstranst.EVENT_DENT_MIC,
					AnalysisTopologyConstranst.EVENT_RELEASE_MIC,
					AnalysisTopologyConstranst.EVENT_LOSTMIC_AUTO,
					AnalysisTopologyConstranst.EVENT_LOSTMIC_REPLACE}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseSpeakEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.GID_FIELD,
					FieldsConstrants.TARGET_FIELD))
			.parallelismHint(2)
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK);

		Stream groupStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(new String[]{
					AnalysisTopologyConstranst.EVENT_JOIN_GROUP,
					AnalysisTopologyConstranst.EVENT_LEAVE_GROUP}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseGroupEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.GID_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_GROUP);

		Stream callStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(new String[]{
					AnalysisTopologyConstranst.EVENT_CALL,
					AnalysisTopologyConstranst.EVENT_QUICKDIAL}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseCallEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.TARGET_GOT_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_CALL);


		Stream queryStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(new String[]{
					AnalysisTopologyConstranst.EVENT_QUERY_USER,
					AnalysisTopologyConstranst.EVENT_QUERY_GROUP,
					AnalysisTopologyConstranst.EVENT_QUERY_CONTACT,
					AnalysisTopologyConstranst.EVENT_QUERY_MEMBERS,
					AnalysisTopologyConstranst.EVENT_QUERY_DEPARTMENT,
					AnalysisTopologyConstranst.EVENT_QUERY_ENTERPISE_GROUP}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseQueryEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.COUNT_FIELD,
					FieldsConstrants.TARGET_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY);

		Stream profileStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter( new String[]{
					AnalysisTopologyConstranst.EVENT_CHANGE_NAME,
					AnalysisTopologyConstranst.EVENT_CHANGE_PWD,
					AnalysisTopologyConstranst.EVENT_CHANGE_PWD_FAILED,
					AnalysisTopologyConstranst.EVENT_CONTACT_REQ,
					AnalysisTopologyConstranst.EVENT_CONTACT_REP,
					AnalysisTopologyConstranst.EVENT_CONTACT_RM}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseProfileEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.TARGET_GOT_FIELD,
					FieldsConstrants.TARGET_DENT_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE);

		Stream manageStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter( new String[]{
					AnalysisTopologyConstranst.EVENT_DISPATCH,
					AnalysisTopologyConstranst.EVENT_SW_GPS,
					AnalysisTopologyConstranst.EVENT_SW_AUDIO,
					AnalysisTopologyConstranst.EVENT_TAKE_MIC,
					AnalysisTopologyConstranst.EVENT_CREATE_GROUP,
					AnalysisTopologyConstranst.EVENT_RM_GROUP,
					AnalysisTopologyConstranst.EVENT_EMPOWER,
					AnalysisTopologyConstranst.EVENT_EMPOWER_FAILED,
					AnalysisTopologyConstranst.EVENT_DEPRIVE,
					AnalysisTopologyConstranst.EVENT_DEPRIVE_FAILED,
					AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME,
					AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME_FAILED}))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseManageEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.TARGET_GOT_FIELD,
					FieldsConstrants.TARGET_DENT_FIELD,
					FieldsConstrants.SW_FIELD,
					FieldsConstrants.VALUE_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE);


		Stream worksheetStream = eventStream.each(
				new Fields(FieldsConstrants.EVENT_FIELD),
				new EventFilter(AnalysisTopologyConstranst.EVENT_WORKSHEET_POST))
			.each(
				new Fields(FieldsConstrants.EVENT_FIELD,FieldsConstrants.CONTENT_FIELD),
				new ParseWorkSheetEvents(),
				new Fields(
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.COUNT_FIELD))
			.name(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_WORKSHEET);

		// log level count
		Stream loadStream = topology.merge(
			logStream.partitionAggregate(
				new Fields(FieldsConstrants.APP_FIELD,FieldsConstrants.DATETIME_FIELD,FieldsConstrants.LEVEL_FIELD),
				new FieldBucketAggregator(FieldsConstrants.APP_FIELD,FieldsConstrants.DATETIME_FIELD,FieldsConstrants.LEVEL_FIELD),
				new Fields(FieldsConstrants.ENTITY_FIELD,FieldsConstrants.BUCKET_FIELD,FieldsConstrants.LOAD_FIELD)),
			eventStream.partitionAggregate(
				new Fields(FieldsConstrants.APP_FIELD,FieldsConstrants.DATETIME_FIELD,FieldsConstrants.EVENT_FIELD),
				new FieldBucketAggregator(FieldsConstrants.APP_FIELD,FieldsConstrants.DATETIME_FIELD,FieldsConstrants.EVENT_FIELD),
				new Fields(FieldsConstrants.ENTITY_FIELD,FieldsConstrants.BUCKET_FIELD,FieldsConstrants.LOAD_FIELD))
		);

		//log.info("loadStream fields: " + Arrays.toString(loadStream.getOutputFields().toList().toArray()));

		TridentState state = loadStream.groupBy(new Fields(FieldsConstrants.ENTITY_FIELD,FieldsConstrants.BUCKET_FIELD)).persistentAggregate(
				EntityLoadState.nonTransactional(new RedisConfig.Builder().setHost("192.168.1.181").setPort(6379).build()),
				new Fields(FieldsConstrants.LOAD_FIELD),
				new EntityLoadAggregator(),
				new Fields(FieldsConstrants.ENTITY_LOAD_FIELD)
				);

		return topology.build();
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException,InterruptedException {
		if( args == null || args.length < 1 ) {
			System.out.println("need cluster or local args");
			return;
		}

		Config conf = new Config();
		String name = AnalysisTopology.class.getSimpleName();

		if( args[0].equalsIgnoreCase("cluster") ) {
			conf.setNumWorkers(AnalysisTopologyConstranst.TOPOLOGY_WORKERS);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, buildTopology());
		} else if( args[0].equalsIgnoreCase("local") ) {
			conf.setDebug(true);
			System.out.println("Submit Topology");

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf,buildTopology());

			System.out.println("Submit successed");
			Thread.sleep(60000);

			System.out.println("shutdown...");
			cluster.shutdown();
		}

		////////////////////////////////////////////////////////////////
		/*
		SpoutConfig spoutConf = new SpoutConfig(
				new ZkHosts(AnalysisTopologyConstranst.ZOOKEEPER_HOST_LIST), 
				AnalysisTopologyConstranst.KAFKA_TOPIC,
				AnalysisTopologyConstranst.ZOOKEEPER_ROOT,
				AnalysisTopologyConstranst.ZOOKEEPER_PTTSVC_LOG_SPOUT_ID);

		spoutConf.scheme = new SchemeAsMultiScheme(new PttsvcLogInfoScheme());
		spoutConf.startOffsetTime = -1; // Start from newest messages.

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(AnalysisTopologyConstranst.SPOUT_INPUT, new KafkaSpout(spoutConf),AnalysisTopologyConstranst.SPORT_INPUT_EXECUTORS); 
		//builder.setBolt("word-splitter", new KafkaWordSplitter(), 2).shuffleGrouping("kafka-reader");
		builder.setBolt(AnalysisTopologyConstranst.BOLT_LOG_SPLITER, new PttsvcLogSpliter(),AnalysisTopologyConstranst.BOLT_EVENT_EXECUTORS)
			.setNumTasks(AnalysisTopologyConstranst.BOLT_EVENT_TASKS)
			.fieldsGrouping(AnalysisTopologyConstranst.SPOUT_INPUT, new Fields(FieldsConstrants.APP_FIELD));


		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost("192.168.1.181").setPort(6379).build();
		builder.setBolt(AnalysisTopologyConstranst.BOLT_STATISTICS_PERSIST,new StatisticsPersist(poolConfig))
			.shuffleGrouping(AnalysisTopologyConstranst.BOLT_LOG_SPLITER,AnalysisTopologyConstranst.STREAM_APP_LOAD);


		Config conf = new Config();
		conf.setNumWorkers(AnalysisTopologyConstranst.TOPOLOGY_WORKERS);

		String name = AnalysisTopology.class.getSimpleName();
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else {
			conf.setDebug(true);
			System.out.println("Create LocalCluster");
			LocalCluster cluster = new LocalCluster();

			System.out.println("Submit Topology");
			cluster.submitTopology(name, conf, builder.createTopology());
			System.out.println("Submit successed");

			Thread.sleep(60000);

			System.out.println("shutdown...");
			cluster.shutdown();
		}
		*/
	}
}

