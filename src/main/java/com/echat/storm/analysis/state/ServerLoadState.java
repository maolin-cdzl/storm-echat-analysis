package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
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
import com.echat.storm.analysis.constant.*;

public class ServerLoadState implements IBackingMap<ServerLoadBucket> {
	private static final Logger log = LoggerFactory.getLogger(ServerLoadState.class);
	private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>() {{
        put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }};

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

			CachedMap c = new CachedMap(new ServerLoadState(jedisPool,_opts.keyPrefix,_opts.window,serializer),_opts.localCacheSize);

			MapState ms;
			if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.<ServerLoadBucket>build(c);
				/*
            } else if(_type==StateType.OPAQUE) {
                ms = OpaqueMap.<ServerLoadBucket>build(c);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.<ServerLoadBucket>build(c);
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
	private ServerLoadMap			_loadmap;
	private int						_window;
	private Gson					_gson;

	public ServerLoadState(JedisPool pool,String keyPrefix,int window,Serializer<ServerLoadBucket> serializer) {
		_pool = pool;
		_keyPrefix = keyPrefix;
		_serializer = serializer;
		_window = window;
		_loadmap = new ServerLoadMap();
		_gson = new GsonBuilder().setDateFormat(TopologyConstant.STD_DATETIME_FORMAT).create();
	}

	@Override
	public List<ServerLoadBucket> multiGet(List<List<Object>> keys) {
		List<ServerLoadBucket> ret = new ArrayList<ServerLoadBucket>(keys.size());
		for(List<Object> key: keys) {
			String server = key.get(0).toString();
			String datetime = key.get(1).toString();
			
			Date time;
			try {
				time = DateUtils.parseDate(datetime,TopologyConstant.INPUT_DATETIME_FORMAT);
			} catch( ParseException e ) {
				log.warn("Bad datetime format: " + datetime);
				continue;
			}
			ret.add( _loadmap.find(server,time) );
		}
		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys,List<ServerLoadBucket> vals) {
		LinkedList<ServerLoadBucket> toPubs = new LinkedList<ServerLoadBucket>();
		for(ServerLoadBucket b : vals) {
			if( b == null || b.server == null ) {
				continue;
			}
			ServerLoadList list = _loadmap.find(b.server);
			if( list == null ) {
				_loadmap.merge(b);
				continue;
			}
			if( list.isTooOld(b.time) ) {
				log.warn("Recv too old report for: "
						+ DateFormatUtils.format(b.time,TopologyConstant.STD_DATETIME_FORMAT) 
						+ ", current earliest is: "
						+ DateFormatUtils.format(list.getEarliestDate(),TopologyConstant.STD_DATETIME_FORMAT));
				continue;
			}
			list.merge(b);

			while( list.getWindow() >= _window ) {
				toPubs.add(list.popEarliest());
			}
		}

		if( toPubs.size() > 0 ) {
			pubToRedis(toPubs);
		}
	}

	public void pubToRedis(List<ServerLoadBucket> buckets) {
		Jedis jedis = null;
		try {
			jedis = _pool.getResource();
			Pipeline pipe = jedis.pipelined();

			for(ServerLoadBucket bucket : buckets) {
				String[] reports = bucket.toReport(_gson,TopologyConstant.STD_DATETIME_FORMAT);
				if( reports != null ) {
					final String server = reports[0];
					final String json = reports[2];

					if( json == null || json.isEmpty() ) {
						return;
					}
					pipe.publish(_keyPrefix + server,json);
				}
			}
			pipe.sync();
		} finally {
			if( jedis != null ) {
				jedis.close();
			}
		}
		//log.info("Publish to " + _keyPrefix + server + ": " + json);
	}
}

