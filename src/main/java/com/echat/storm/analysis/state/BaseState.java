package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class BaseState implements State {
	private static final String OUTPUT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
    }

    public static class Factory implements StateFactory {
        public static final redis.clients.jedis.JedisPoolConfig DEFAULT_POOL_CONFIG = new redis.clients.jedis.JedisPoolConfig();

        private RedisConfig config;
		private int maxCacheSize;

        public Factory(RedisConfig config) {
			this(config,30000);
        }

		public Factory(RedisConfig config,int maxCacheSize) {
			this.config = config;
			this.maxCacheSize = maxCacheSize;
		}

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,config.host,config.port);
            return new BaseState(jedisPool,maxCacheSize);
        }
    }

    private JedisPool jedisPool;
	private Gson gson;
	private LRUHashMap<String,Long> timelineMap;

    public BaseState(JedisPool jedisPool,int maxCacheSize) {
        this.jedisPool = jedisPool;
		timelineMap = new LRUHashMap<String,Long>(maxCacheSize);
		gson = new GsonBuilder().setDateFormat(OUTPUT_DATETIME_FORMAT).build();
    }

    public Jedis getJedis() {
        return this.jedisPool.getResource();
    }

    public void returnJedis(Jedis jedis) {
        jedis.close();
    }

	public Gson getGson() {
		return gson;
	}

	public Long getTimeline(String type,String id) {
		final String key = type + id;
		return timelineMap.get(key);
	}

	public boolean updateTimeline(String type,String id,Long tl) {
		final String key = type + id;

		Long last = timelineMap.get(key);
		if( last == null || last <= tl ) {
			timelineMap.put(key,tl);
			return true;
		} else {
			return false;
		}
	}

	public boolean isTooOld(String type,String id,Long tl) {
		final String key = type + id;
		Long last = timelineMap.get(key);
		if( last != null && last > tl ) {
			return true;
		} else {
			return false;
		}
	}

}

