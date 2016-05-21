package com.echat.storm.analysis;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.oro.text.regex.MalformedPatternException;

import java.util.Arrays;

public class AnalysisTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException,InterruptedException, MalformedPatternException {
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
	}
}

