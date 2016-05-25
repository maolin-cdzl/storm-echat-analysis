package com.echat.storm.analysis;

public class AnalysisTopologyConstranst {
	// debug
	public static final boolean DEBUG = true;

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

	public static final String BOLT_LOG_SPLITER = "log-spliter";
	public static final int BOLT_EVENT_EXECUTORS = 3;
	public static final int BOLT_EVENT_TASKS = TOPOLOGY_WORKERS * BOLT_EVENT_EXECUTORS;

	public static final String STREAM_APP_LOAD = "app-load";
	public static final String STREAM_WARN = "warn";
	public static final String STREAM_ERROR = "error";
	public static final String STREAM_FATAL = "fatal";

	public static final String STREAM_EVENT_GROUP_ONLINE = "event-online";
	public static final String STREAM_EVENT_GROUP_LOGIN_FAILED = "event-login-failed";
	public static final String STREAM_EVENT_GROUP_QUERY = "event-query";
	public static final String STREAM_EVENT_GROUP_GROUP = "event-group";
	public static final String STREAM_EVENT_GROUP_SPEAK = "event-speak";
	public static final String STREAM_EVENT_GROUP_CALL = "event-call";
	public static final String STREAM_EVENT_GROUP_PROFILE = "event-profile";
	public static final String STREAM_EVENT_GROUP_MANAGE = "event-manage";
	public static final String STREAM_EVENT_GROUP_WORKSHEET = "event-worksheet";

	// speak
	public static final String EVENT_GET_MIC = "GET_MIC";
	public static final String EVENT_RELEASE_MIC = "RELEASE_MIC";
	public static final String EVENT_DENT_MIC = "DENT_MIC";
	public static final String EVENT_LOSTMIC_AUTO = "LOSTMIC_AUTO";
	public static final String EVENT_LOSTMIC_REPLACE = "LOSTMIC_REPLACE";

	// online
	public static final String EVENT_RELOGIN = "RELOGIN";
	public static final String EVENT_LOGIN = "LOGIN";
	public static final String EVENT_LOGOUT = "LOGOUT";
	public static final String EVENT_BROKEN = "BROKEN";

	// login failed
	public static final String EVENT_LOGIN_FAILED = "LOGIN_FAILED";

	// group in out
	public static final String EVENT_JOIN_GROUP = "JOIN_GROUP";
	public static final String EVENT_LEAVE_GROUP = "LEAVE_GROUP";

	// call
	public static final String EVENT_CALL = "CALL";
	public static final String EVENT_QUICKDIAL = "QUICK_DIAL";

	// query
	public static final String EVENT_QUERY_MEMBERS = "QUERY_MEMBERS";
	public static final String EVENT_QUERY_GROUP = "QUERY_GROUP";
	public static final String EVENT_QUERY_CONTACT = "QUERY_CONTACT";
	public static final String EVENT_QUERY_USER = "QUERY_USER";
	public static final String EVENT_QUERY_DEPARTMENT = "QUERY_DEPARTMENT";
	public static final String EVENT_QUERY_ENTERPISE_GROUP = "QUERY_ENTERPRISE_GROUP";

	// profile
	public static final String EVENT_CHANGE_NAME = "CH_NAME";
	public static final String EVENT_CHANGE_PWD = "CH_PWD";
	public static final String EVENT_CHANGE_PWD_FAILED = "CH_PWD_FAILED";
	public static final String EVENT_CONTACT_REQ = "CONTACT_REQ";
	public static final String EVENT_CONTACT_REP = "CONTACT_REP";
	public static final String EVENT_CONTACT_RM = "CONTACT_RM";
	
	// manage
	public static final String EVENT_DISPATCH = "DISPATCH";
	public static final String EVENT_CONFIG = "CONFIG"; // current contains SW_AUDIO and SW_GPS
	public static final String EVENT_SW_AUDIO = "SW_AUDIO";
	public static final String EVENT_SW_GPS = "SW_GPS";
	public static final String EVENT_TAKE_MIC = "TAKE_MIC";
	public static final String EVENT_CREATE_GROUP = "CREATE_GROUP";
	public static final String EVENT_RM_GROUP = "RM_GROUP";
	public static final String EVENT_EMPOWER = "EMPOWER";
	public static final String EVENT_EMPOWER_FAILED = "EMPOWER_FAILED";
	public static final String EVENT_DEPRIVE = "DEPRIVE";
	public static final String EVENT_DEPRIVE_FAILED = "DEPRIVE_FAILED";
	public static final String EVENT_CHANGE_GROUP_NAME = "CH_GROUP_NAME";
	public static final String EVENT_CHANGE_GROUP_NAME_FAILED = "CH_GROUP_NAME_FAILED";
	
	// worksheet
	public static final String EVENT_WORKSHEET_POST = "POST";


	// bolt
	public static final String BOLT_STATISTICS_PERSIST = "bolt-stat-persist";
	public static final String KEY_APP_LOAD_PREFIX = "appload-";
	public static final long MAX_APP_LOAD_LENGTH = 3600;
}

