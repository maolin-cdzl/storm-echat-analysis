package com.echat.storm.analysis.state;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.BaseStateUpdater;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class OnlineUpdater extends BaseStateUpdater<BaseState> {
	private static final String TIMELINE_ONLINE = "online-";
	private static final String TIMELINE_OFFLINE = "offline-";
	private static final Logger log = LoggerFactory.getLogger(OnlineUpdater.class);


	private HashMap<String,List<OnlineEvent>>		_events = new HashMap<String,List<OnlineEvent>>();

	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		log.info("updateState, input tuple count: " + inputs.size());
		for(TridentTuple tuple : inputs) {
			OnlineEvent ev = OnlineEvent.fromTuple(tuple);
			if( ev != null ) {
				put(ev);
			}
		}

		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.pipelined();
			for (List<OnlineEvent> l : _events.values() ) {
				for(OnlineEvent ev : l) {
					if( TopologyConstant.EVENT_LOGIN.equals(ev.event) ) {
						processLogin(state,pipe,collector,ev);
					} else if( TopologyConstant.EVENT_RELOGIN.equals(ev.event) ) {
						processRelogin(state,pipe,collector,ev);
					} else if( TopologyConstant.EVENT_BROKEN.equals(ev.event) ) {
						processBroken(state,pipe,collector,ev);
					} else if( TopologyConstant.EVENT_LOGOUT.equals(ev.event) ) {
						processLogout(state,pipe,collector,ev);
					} else {
						log.error("Unknown event: " + ev.event);
					}
				}
				pipe.sync();
			}
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}

	}

	private void put(OnlineEvent ev) {
		List<OnlineEvent> l = _events.get(ev.uid);
		if( l == null ) {
			l = new SortedLinkedList<OnlineEvent>(OnlineEvent.tsComparator());
			_events.put(ev.uid,l);
		}
		l.add(ev);
	}

	private void processLogin(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_ONLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_OFFLINE,ev.uid,ev.ts) ) {
			setUserOnline(state,pipe,collector,ev);
		}

		String lastLogoutJson = null;
		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			lastLogoutJson = jedis.get(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGOUT_SUFFIX);
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}

		if( lastLogoutJson != null ) {
			OnlineEvent lastLogout = state.getGson().fromJson(lastLogoutJson,OnlineEvent.class);
			if( lastLogout != null ) {
				if( ev.date.after(lastLogout.date) && TimeUnit.MILLISECONDS.toSeconds(ev.date.getTime() - lastLogout.date.getTime()) < 60) {
					stormAndEmitConnectionBroken(state,pipe,collector,lastLogout);
				}
			}
		}
	}

	private void processRelogin(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_ONLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_OFFLINE,ev.uid,ev.ts) ) {
			setUserOnline(state,pipe,collector,ev);
		}

		stormAndEmitConnectionBroken(state,pipe,collector,ev);
	}

	private void processBroken(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_OFFLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGOUT_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_ONLINE,ev.uid,ev.ts) ) {
			setUserOffline(state,pipe,collector,ev);
		}
	}

	private void processLogout(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_OFFLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGOUT_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_ONLINE,ev.uid,ev.ts) ) {
			setUserOffline(state,pipe,collector,ev);
		}
	}

	private void stormAndEmitConnectionBroken(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		String content = state.getGson().toJson(ev);
		pipe.lpush(RedisConstant.BROKEN_LIST_KEY,content);
		pipe.ltrim(RedisConstant.BROKEN_LIST_KEY,0,RedisConstant.BROKEN_LIST_MAX_SIZE);
		collector.emit(new Values(content));
	}

	private void setUserOnline(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.STATE_SUFFIX,RedisConstant.STATE_ONLINE);
		pipe.sadd(RedisConstant.ONLINE_USER_KEY,ev.uid);

		pipe.sadd(RedisConstant.ENTITY_PREFIX + ev.app + RedisConstant.USER_SUFFIX,ev.uid);
		pipe.sadd(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.ENTITY_SET_SUFFIX,ev.app);

		if( ev.device != null ) {
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SUFFIX,ev.device);
			pipe.sadd(RedisConstant.DEVICE_PREFIX + ev.device + RedisConstant.USER_SUFFIX,ev.uid);
			pipe.sadd(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SET_SUFFIX,ev.device);
		}

	}

	private void setUserOffline(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.STATE_SUFFIX,RedisConstant.STATE_OFFLINE);
		pipe.srem(RedisConstant.ONLINE_USER_KEY,ev.uid);

		pipe.srem(RedisConstant.ENTITY_PREFIX + ev.app + RedisConstant.USER_SUFFIX,ev.uid);

		String device = null;
		Jedis jedis = null;
		try {
			device = jedis.get(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SUFFIX);
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}

		if( device != null ) {
			pipe.srem(RedisConstant.DEVICE_PREFIX + device + RedisConstant.USER_SUFFIX,ev.uid);
			pipe.del(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SUFFIX);
		}
	}
}

