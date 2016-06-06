package com.echat.storm.analysis.constant;

import com.echat.storm.analysis.types.RedisConfig;

public class TopologyConstant {
	// debug
	public static final boolean DEBUG = true;
	public static final String STD_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String[] INPUT_DATETIME_FORMAT = new String[] { 
		"yyyy-MM-dd HH:mm:ss.SSS",
		"yyyy/MM/dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss"
   	};
	public static final RedisConfig REDIS_CONFIG = new RedisConfig.Builder().setHost("192.168.1.181").setPort(6379).build();

	// zookeeper
	public static final String ZOOKEEPER_HOST_LIST = "base001.hdp.echat.com:2181,base002.hdp.echat.com:2181,base003.hdp.echat.com:2181";
	public static final String ZOOKEEPER_ROOT = "/kafkastorm";
	public static final String ZOOKEEPER_PTTSVC_LOG_SPOUT_ID = "spout-loginfo";

	// kafka
	public static final String KAFKA_TOPIC = "pttsvc-loginfo";
	public static final int KAFKA_TOPIC_PARTITION = 3;
	public static final int STORM_MACHINES_NUMBER = 4;
	public static final int STORM_WORKERS_NUMBER = 2 * STORM_MACHINES_NUMBER;



	// spout and bolts
	public static final int TOPOLOGY_WORKERS = 3;
	public static final String SPOUT_INPUT = ZOOKEEPER_PTTSVC_LOG_SPOUT_ID;
	public static final int SPORT_INPUT_EXECUTORS = KAFKA_TOPIC_PARTITION;

	public static final String BOLT_LOG_SPLITER = "log-spliter";
	public static final int BOLT_EVENT_EXECUTORS = 3;
	public static final int BOLT_EVENT_TASKS = BOLT_EVENT_EXECUTORS;

	public static final String STREAM_SERVER_LOAD = "s-entityload";
	public static final String STREAM_WARN = "s-warn";
	public static final String STREAM_ERROR = "s-error";
	public static final String STREAM_FATAL = "s-fatal";
	public static final String STREAM_INFO = "s-info";
	public static final String STREAM_EVENT = "s-event";

	public static final String STREAM_EVENT_GROUP_ONLINE = "s-online";
	public static final String STREAM_EVENT_GROUP_LOGIN_FAILED = "s-login-failed";
	public static final String STREAM_EVENT_GROUP_QUERY = "s-query";
	public static final String STREAM_EVENT_GROUP_GROUP = "s-group";
	public static final String STREAM_EVENT_GROUP_SPEAK = "s-speak";
	public static final String STREAM_EVENT_GROUP_CALL = "s-call";
	public static final String STREAM_EVENT_GROUP_PROFILE = "s-profile";
	public static final String STREAM_EVENT_GROUP_MANAGE = "s-manage";
	public static final String STREAM_EVENT_GROUP_WORKSHEET = "s-worksheet";


	// bolt
	public static final String BOLT_STATISTICS_PERSIST = "bolt-stat-persist";
	public static final String KEY_SERVER_LOAD_PREFIX = "entityload-";
	public static final long MAX_SERVER_LOAD_LENGTH = 3600;

	public static final int LOAD_SPLIDING_WINDOW_IN_SECONDS = 5;
}

