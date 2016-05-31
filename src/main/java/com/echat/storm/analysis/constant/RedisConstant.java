package com.echat.storm.analysis.constant;

public class RedisConstant {
	static public final String ONLINE_USER_KEY = "online-user";
	static public final String ENTITY_SET_KEY = "entity-set";
	static public final String DEVICE_SET_KEY = "dev-set";

	static public final String ENTITY_PREFIX = "app:";
	static public final String DEVICE_PREFIX = "dev:";
	static public final String USER_PREFIX = "user:";



	static public final String USER_SUFFIX = ":user";
	static public final String STATE_SUFFIX = ":state";
	static public final String ENTITY_SUFFIX = ":entity";
	static public final String DEVICE_SUFFIX = ":dev";
	static public final String LAST_LOGIN_SUFFIX = ":last-login";
	static public final String LAST_LOGOUT_SUFFIX = ":last-logout";
	static public final String ENTITY_SET_SUFFIX = ":entity-set";
	static public final String DEVICE_SET_SUFFIX = ":dev-set";

	static public final String SESSION_LIST_SUFFIX = ":sessions";
	static public final int SESSION_LIST_MAX_SIZE = 100;

	static public final String BROKEN_LIST_SUFFIX = "brokens";
	static public final int BROKEN_LIST_MAX_SIZE = 100;


	static public final String STATE_ONLINE = "online";
	static public final String STATE_OFFLINE = "offline";
}


