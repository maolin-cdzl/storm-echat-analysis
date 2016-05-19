package com.echat.storm.analysis;

public class AnalysisTopologyConstranst {
	// zookeeper
	public static final String ZOOKEEPER_HOST_LIST = "base001.hdp.echat.com:2181,base002.hdp.echat.com:2181,base003.hdp.echat.com:2181";
	public static final String ZOOKEEPER_ROOT = "/kafkastorm";
	public static final String ZOOKEEPER_PTTSVC_LOG_SPOUT_ID = "spout-loginfo";

	// kafka
	public static final String KAFKA_TOPIC = "pttsvc-loginfo";
	public static final int KAFKA_TOPIC_PARTITION = 3;



	// spout and bolts
	public static final int TOPOLOGY_WORKERS = 3;
	public static final String SPOUT_INPUT = ZOOKEEPER_PTTSVC_LOG_SPOUT_ID;
	public static final int SPORT_INPUT_EXECUTORS = KAFKA_TOPIC_PARTITION;

	public static final String BOLT_ACTION = "bolt-action";
	public static final int BOLT_ACTION_EXECUTORS = 3;
	public static final int BOLT_ACTION_TASKS = TOPOLOGY_WORKERS * BOLT_ACTION_EXECUTORS;
}

