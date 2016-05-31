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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class BaseState implements State {
	private static final Logger log = LoggerFactory.getLogger(BaseState.class);

    @Override
    public void beginCommit(Long txid) {
		log.info("beginCommit txid: " + txid.toString());
    }

    @Override
    public void commit(Long txid) {
		log.info("commit txid: " + txid.toString());
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
		gson = new GsonBuilder().setDateFormat(TopologyConstant.STD_DATETIME_FORMAT).create();
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

	public boolean timelineBefore(String id,String type1,String type2) {
		Long t1 = getTimeline(type1,id);
		Long t2 = getTimeline(type2,id);
		if( t2 == null ) {
			return true;
		} else if( t1 == null ) {
			return false;
		} else {
			return t1 < t2;
		}
	}

}

