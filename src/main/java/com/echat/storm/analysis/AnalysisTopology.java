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

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.operation.*;
import com.echat.storm.analysis.spout.*;
import com.echat.storm.analysis.state.*;

public class AnalysisTopology {
	private static final Logger log = LoggerFactory.getLogger(AnalysisTopology.class);

	private static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(
				new ZkHosts(TopologyConstant.ZOOKEEPER_HOST_LIST), 
				TopologyConstant.KAFKA_TOPIC,
				TopologyConstant.ZOOKEEPER_PTTSVC_LOG_SPOUT_ID
				);
		spoutConf.scheme = new SchemeAsMultiScheme(new PttsvcLogInfoScheme());
		spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime(); 
		//spoutConf.ignoreZkOffsets = true;

		Stream logStream = topology.newStream(TopologyConstant.SPOUT_INPUT,new OpaqueTridentKafkaSpout(spoutConf)).partitionBy(new Fields(FieldConstant.ENTITY_FIELD)).parallelismHint(TopologyConstant.SPORT_INPUT_EXECUTORS); 

		Stream fatalStream = logStream.each(new Fields(FieldConstant.LEVEL_FIELD),new LevelFilter("FATAL")).name(TopologyConstant.STREAM_FATAL);
		Stream errorStream = logStream.each(new Fields(FieldConstant.LEVEL_FIELD),new LevelFilter("ERROR")).name(TopologyConstant.STREAM_ERROR);
		Stream warnStream = logStream.each(new Fields(FieldConstant.LEVEL_FIELD),new LevelFilter("WARNING")).name(TopologyConstant.STREAM_WARN);
		Stream infoStream = logStream.each(new Fields(FieldConstant.LEVEL_FIELD),new LevelFilter("INFO")).name(TopologyConstant.STREAM_INFO);

		// event streams
		Stream eventStream = infoStream.each(new Fields(FieldConstant.CONTENT_FIELD),new GetEvent(),new Fields(FieldConstant.EVENT_FIELD));

		Stream onlineStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(new String[]{
					TopologyConstant.EVENT_LOGIN,
					TopologyConstant.EVENT_RELOGIN,
					TopologyConstant.EVENT_BROKEN,
					TopologyConstant.EVENT_LOGOUT}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseOnlineEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.CTX_FIELD,
					FieldConstant.IP_FIELD,
					FieldConstant.DEVICE_FIELD,
					FieldConstant.DEVICE_ID_FIELD,
					FieldConstant.VERSION_FIELD,
					FieldConstant.IMSI_FIELD,
					FieldConstant.EXPECT_PAYLOAD_FIELD,
					FieldConstant.COMPANY_FIELD,
					FieldConstant.AGENT_FIELD,
					))
			.name(TopologyConstant.STREAM_EVENT_GROUP_ONLINE);

		GroupedStream loginFailedStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(TopologyConstant.EVENT_LOGIN_FAILED))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseLoginFailedEvents(),
				new Fields(
					FieldConstant.REASON_FIELD,
					FieldConstant.UID_FIELD,
					FieldConstant.CTX_FIELD,
					FieldConstant.IP_FIELD,
					FieldConstant.DEVICE_FIELD,
					FieldConstant.DEVICE_ID_FIELD,
					FieldConstant.VERSION_FIELD,
					FieldConstant.IMSI_FIELD,
					FieldConstant.EXPECT_PAYLOAD_FIELD))
			.groupBy(new Fields(FieldConstant.UID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_LOGIN_FAILED);
			
		GroupedStream speakStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(new String[]{
					TopologyConstant.EVENT_GET_MIC,
					TopologyConstant.EVENT_DENT_MIC,
					TopologyConstant.EVENT_RELEASE_MIC,
					TopologyConstant.EVENT_LOSTMIC_AUTO,
					TopologyConstant.EVENT_LOSTMIC_REPLACE}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseSpeakEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.GID_FIELD,
					FieldConstant.TARGET_FIELD))
			.parallelismHint(2)
			.groupBy(new Fields(FieldConstant.GID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_SPEAK);

		GroupedStream groupStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(new String[]{
					TopologyConstant.EVENT_JOIN_GROUP,
					TopologyConstant.EVENT_LEAVE_GROUP}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseGroupEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.GID_FIELD))
			.groupBy(new Fields(FieldConstant.GID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_GROUP);

		GroupedStream callStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(new String[]{
					TopologyConstant.EVENT_CALL,
					TopologyConstant.EVENT_QUICKDIAL}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseCallEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.TARGET_FIELD,
					FieldConstant.TARGET_GOT_FIELD))
			.groupBy(new Fields(FieldConstant.UID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_CALL);


		GroupedStream queryStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(new String[]{
					TopologyConstant.EVENT_QUERY_USER,
					TopologyConstant.EVENT_QUERY_GROUP,
					TopologyConstant.EVENT_QUERY_CONTACT,
					TopologyConstant.EVENT_QUERY_MEMBERS,
					TopologyConstant.EVENT_QUERY_DEPARTMENT,
					TopologyConstant.EVENT_QUERY_ENTERPISE_GROUP}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseQueryEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.COUNT_FIELD,
					FieldConstant.TARGET_FIELD))
			.groupBy(new Fields(FieldConstant.UID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_QUERY);

		GroupedStream profileStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter( new String[]{
					TopologyConstant.EVENT_CHANGE_NAME,
					TopologyConstant.EVENT_CHANGE_PWD,
					TopologyConstant.EVENT_CHANGE_PWD_FAILED,
					TopologyConstant.EVENT_CONTACT_REQ,
					TopologyConstant.EVENT_CONTACT_REP,
					TopologyConstant.EVENT_CONTACT_RM}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseProfileEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.TARGET_FIELD,
					FieldConstant.TARGET_GOT_FIELD,
					FieldConstant.TARGET_DENT_FIELD))
			.groupBy(new Fields(FieldConstant.UID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_PROFILE);

		GroupedStream manageStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter( new String[]{
					TopologyConstant.EVENT_DISPATCH,
					TopologyConstant.EVENT_SW_GPS,
					TopologyConstant.EVENT_SW_AUDIO,
					TopologyConstant.EVENT_TAKE_MIC,
					TopologyConstant.EVENT_CREATE_GROUP,
					TopologyConstant.EVENT_RM_GROUP,
					TopologyConstant.EVENT_EMPOWER,
					TopologyConstant.EVENT_EMPOWER_FAILED,
					TopologyConstant.EVENT_DEPRIVE,
					TopologyConstant.EVENT_DEPRIVE_FAILED,
					TopologyConstant.EVENT_CHANGE_GROUP_NAME,
					TopologyConstant.EVENT_CHANGE_GROUP_NAME_FAILED}))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseManageEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.TARGET_FIELD,
					FieldConstant.TARGET_GOT_FIELD,
					FieldConstant.TARGET_DENT_FIELD,
					FieldConstant.SW_FIELD,
					FieldConstant.VALUE_FIELD))
			.groupBy(new Fields(FieldConstant.UID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_MANAGE);


		GroupedStream worksheetStream = eventStream.each(
				new Fields(FieldConstant.EVENT_FIELD),
				new EventFilter(TopologyConstant.EVENT_WORKSHEET_POST))
			.each(
				new Fields(FieldConstant.EVENT_FIELD,FieldConstant.CONTENT_FIELD),
				new ParseWorkSheetEvents(),
				new Fields(
					FieldConstant.UID_FIELD,
					FieldConstant.TARGET_FIELD,
					FieldConstant.COUNT_FIELD))
			.groupBy(new Fields(FieldConstant.UID_FIELD))
			.name(TopologyConstant.STREAM_EVENT_GROUP_WORKSHEET);

		// log level count
		Stream loadStream = topology.merge(
			logStream.partitionAggregate(
				new Fields(FieldConstant.ENTITY_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.LEVEL_FIELD),
				new FieldBucketAggregator(FieldConstant.ENTITY_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.LEVEL_FIELD),
				new Fields(FieldConstant.ENTITY_FIELD,FieldConstant.BUCKET_FIELD,FieldConstant.LOAD_FIELD)),
			eventStream.partitionAggregate(
				new Fields(FieldConstant.ENTITY_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.EVENT_FIELD),
				new FieldBucketAggregator(FieldConstant.ENTITY_FIELD,FieldConstant.DATETIME_FIELD,FieldConstant.EVENT_FIELD),
				new Fields(FieldConstant.ENTITY_FIELD,FieldConstant.BUCKET_FIELD,FieldConstant.LOAD_FIELD))
		);


		//log.info("loadStream fields: " + Arrays.toString(loadStream.getOutputFields().toList().toArray()));
		TridentState loadState = loadStream.groupBy(new Fields(FieldConstant.ENTITY_FIELD,FieldConstant.BUCKET_FIELD)).persistentAggregate(
				EntityLoadState.nonTransactional(TopologyConstant.REDIS_CONFIG),
				new Fields(FieldConstant.LOAD_FIELD),
				new EntityLoadAggregator(),
				new Fields(FieldConstant.ENTITY_LOAD_FIELD)
				);

		onlineStream.partitionPersist(
				new BaseState.Factory(TopologyConstant.REDIS_CONFIG),
				new Fields(FieldConstant.ENTITY_FIELD,FieldConstant.DEVICE_FIELD),
				new EntityDevUpdater(),
				new Fields()
				);

		TridentState onlineState = onlineStream.partitionPersist(
				new BaseState.Factory(TopologyConstant.REDIS_CONFIG),
				onlineStream.getOutputFields(),
				new OnlineUpdater(),
				new Fields(FieldConstant.BROKEN_EVENT_FIELD)
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
			conf.setNumWorkers(TopologyConstant.TOPOLOGY_WORKERS);
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

	}
}

