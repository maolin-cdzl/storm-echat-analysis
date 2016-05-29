package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;
import java.text.ParseException;

import backtype.storm.tuple.Values;
import backtype.storm.task.IMetricsContext;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.CachedMap;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.types.*;

public class EntityLoadState implements IBackingMap<EntityLoadBucket> {
	private static final Logger log = LoggerFactory.getLogger(EntityLoadState.class);
	private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>() {{
        put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }};
	private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final String[] INPUT_DATETIME_FORMAT = { "yyyy-MM-dd HH:mm:ss" };

	public static class Options implements Serializable {
		public String keyPrefix = "load-";
		public Serializer serializer = null;
		public int localCacheSize = 1024;
		public int window = 5;
	}

	public static StateFactory nonTransactional(RedisConfig conf) {
		return nonTransactional(conf,new Options());
	}

	public static StateFactory nonTransactional(RedisConfig conf,Options opts) {
		return new Factory(conf,StateType.NON_TRANSACTIONAL,opts);
	}

	protected static class Factory implements StateFactory {
		public static final JedisPoolConfig DEFAULT_POOL_CONFIG = new JedisPoolConfig();
		private RedisConfig _conf;
		private StateType _type;
		private Options _opts;

		public Factory(RedisConfig conf,StateType type,Options opts) {
			_conf = conf;
			_type = type;
			_opts = opts;
		}

		@Override
		public State makeState(Map conf, IMetricsContext context, int partitionIndex, int numPartitions) {
			JedisPool jedisPool = new JedisPool(DEFAULT_POOL_CONFIG,_conf.host,_conf.port);
			Serializer serializer = getSerializer();

			CachedMap c = new CachedMap(new EntityLoadState(jedisPool,_opts.keyPrefix,_opts.window,serializer),_opts.localCacheSize);

			MapState ms;
			if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.<EntityLoadBucket>build(c);
				/*
            } else if(_type==StateType.OPAQUE) {
                ms = OpaqueMap.<EntityLoadBucket>build(c);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.<EntityLoadBucket>build(c);
				*/
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }

            return new SnapshottableMap(ms, new Values());
		}

		private Serializer getSerializer() {
			Serializer ser;
			if( _opts.serializer == null ) {
				ser = DEFAULT_SERIALZERS.get(_type);
				if( ser == null ) {
					throw new RuntimeException("Couldn't find serializer for state type: " + _type);
				}
			} else {
				ser = _opts.serializer;
			}
			return ser;
		}
	}

	private String					_keyPrefix;
	private JedisPool				_pool;
	private Serializer				_serializer;
	private EntityLoadMap			_loadmap;
	private int						_window;
	private Gson					_gson;

	public EntityLoadState(JedisPool pool,String keyPrefix,int window,Serializer<EntityLoadBucket> serializer) {
		_pool = pool;
		_keyPrefix = keyPrefix;
		_serializer = serializer;
		_window = window;
		_loadmap = new EntityLoadMap();
		_gson = new GsonBuilder().setDateFormat(DATETIME_FORMAT).create();
	}

	@Override
	public List<EntityLoadBucket> multiGet(List<List<Object>> keys) {
		List<EntityLoadBucket> ret = new ArrayList<EntityLoadBucket>(keys.size());
		for(List<Object> key: keys) {
			String entity = key.get(0).toString();
			String datetime = key.get(1).toString();
			
			Date time;
			try {
				time = DateUtils.parseDate(datetime,INPUT_DATETIME_FORMAT);
			} catch( ParseException e ) {
				log.warn("Bad datetime format: " + datetime);
				continue;
			}
			ret.add( _loadmap.find(entity,time) );
		}
		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys,List<EntityLoadBucket> vals) {
		for(EntityLoadBucket b : vals) {
			if( b == null || b.entity == null ) {
				continue;
			}
			EntityLoadList list = _loadmap.find(b.entity);
			if( list == null ) {
				_loadmap.merge(b);
				continue;
			}
			if( list.isTooOld(b.time) ) {
				log.warn("Recv too old report for: "
						+ DateFormatUtils.format(b.time,DATETIME_FORMAT) 
						+ ", current earliest is: "
						+ DateFormatUtils.format(list.getEarliestDate(),DATETIME_FORMAT));
				continue;
			}
			list.merge(b);

			while( list.getWindow() >= _window ) {
				pubToRedis(list.popEarliest());
			}
		}
	}

	public void pubToRedis(EntityLoadBucket bucket) {
		String[] reports = bucket.toReport(_gson,DATETIME_FORMAT);
		if( reports == null ) {
			return;
		}
		final String entity = reports[0];
		final String json = reports[2];

		if( json == null || json.isEmpty() ) {
			return;
		}

		Jedis jedis = null;
		try {
			jedis = _pool.getResource();
			jedis.publish(_keyPrefix + entity,json);
		} finally {
			if( jedis != null ) {
				jedis.close();
			}
		}
		//log.info("Publish to " + _keyPrefix + entity + ": " + json);
	}
}

