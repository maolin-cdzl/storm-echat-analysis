package com.echat.storm.analysis.utils;

import redis.clients.jedis.Jedis;

import com.echat.storm.analysis.types.RedisConfig;

public class UserOrgInfoReader {
	static private final long TIMEOUT_MILLIS = 5 * 60 * 1000; // 5 minute
	public class OrganizationInfo {
		public String			company;
		public String			agent;
	}

	private class TimedOrganizationInfo {
		public long					expired;
		public OrganizationInfo		info;
	}

    private Jedis jedis;
	private LRUHashMap<String,TimedOrganizationInfo> cache;
	
	public UserOrgInfoReader(RedisConfig config) {
		jedis = new Jedis(config.host,config.port,config.timeout);
		cache = new LRUHashMap<String,TimedOrganizationInfo>(10000);
	}

	public OrganizationInfo search(final String uid) {
		final long now = System.currentTimeMillis();
		TimedOrganizationInfo ti = cache.get(uid);

		// cache it even if not found.
		if( ti != null ) {
			if( ti.expired >= now ) {
				ti.info = readFromRedis(uid);
				ti.expired = now + TIMEOUT_MILLIS;
			}
		} else {
			ti = new TimedOrganizationInfo();
			ti.expired = now + TIMEOUT_MILLIS;
			ti.info = readFromRedis(uid);

			cache.put(uid,ti);
		}
		return ti.info;
	}

	private OrganizationInfo readFromRedis(String uid) {
		String company = jedis.get("db:user:" + uid + ":company");
		if( company != null ) {
			OrganizationInfo info = new OrganizationInfo();
			info.company = company;
			info.agent = jedis.get("db:company:" + company + ":agent");
			return info;
		} else {
			return null;
		}
	}
}

