package com.echat.storm.analysis.state;

import java.util.List;
import java.util.Set;
import java.util.HashSet;


import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.BaseStateUpdater;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;

public class EntityDevUpdater extends BaseStateUpdater<BaseState> {
	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		HashSet<String> entities = new HashSet<String>();
		HashSet<String> devices = new HashSet<String>();

		String e;
		String d;

		for(TridentTuple tuple : inputs) {
			e = tuple.getStringByField(FieldConstant.ENTITY_FIELD);
			if( e != null ) {
				entities.add( e);
			}
			if( tuple.contains(FieldConstant.DEVICE_FIELD) ) {
				d = tuple.getStringByField(FieldConstant.DEVICE_FIELD);
				if( d != null ) {
					devices.add( d );
				}
			}
		}

		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.pipelined();
			for(String entity : entities) {
				pipe.sadd(RedisConstant.ENTITY_SET_KEY,entity);
			}
			for(String dev : devices) {
				pipe.sadd(RedisConstant.DEVICE_SET_KEY,dev);
			}
			pipe.sync();
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}
	}
}

