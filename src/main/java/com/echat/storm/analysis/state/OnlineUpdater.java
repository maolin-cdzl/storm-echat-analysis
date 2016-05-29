package com.echat.storm.analysis.state;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

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
	private static final String[] INPUT_DATETIME_FORMAT = {
		"yyyy-MM-dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss.SSS",
	};
	private static final String TIMELINE_ONLINE = "online-";
	private static final String TIMELINE_OFFLINE = "offline-";
	private static final Logger log = LoggerFactory.getLogger(OnlineUpdater.class);


	private HashMap<String,List<OnlineEvent>>		_events = new HashMap<String,List<OnlineEvent>>();

	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		for(TridentTuple tuple : inputs) {
			OnlineEvent ev = OnlineEvent.fromTuple(tuple);
			if( ev != null ) {
				put(ev);
			}
		}

		Jedis jedis;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.piplined();
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
			}
			pipe.sync();
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}

	}

	private void put(OnlineEvent ev) {
		List<OnlineEvent> l = _events.get(ev.uid);
		if( l == null ) {
			l = new SortedList<OnlineEvent>(OnlineEvent.tsComparator());
			_events.put(ev.uid,l);
		}
		l.add(ev);
	}

	private void processLogin(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_ONLINE,ev.uid,ev.ts) ) {
			String val = state.getGson().toJson(ev);
			pipe.set(RedisConstranst.USER_PREFIX + ev.uid + RedisConstranst.ONLINE_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_OFFLINE,ev,uid,ev.ts) ) {
			// set user status to online
			pipe.sadd(RedisConstranst.ONLINE_USER_KEY,ev.uid);
			pipe.sadd(RedisConstranst.APP_PREFIX + ev.app + RedisConstranst.ONLINE_SUFFIX,ev.uid);
			if( ev.device != null ) {
				pipe.sadd(RedisConstranst.DEVICE_PREFIX + ev.device + RedisConstranst.USER_SUFFIX,ev.uid);
			}
		}
	}

	private void processRelogin(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_ONLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstranst.USER_PREFIX + ev.uid + RedisConstranst.ONLINE_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_OFFLINE,ev,uid,ev.ts) ) {
			// set user status to online
			pipe.sadd(RedisConstranst.ONLINE_USER_KEY,ev.uid);
			pipe.sadd(RedisConstranst.APP_PREFIX + ev.app + RedisConstranst.ONLINE_SUFFIX,ev.uid);
			if( ev.device != null ) {
				pipe.sadd(RedisConstranst.DEVICE_PREFIX + ev.device + RedisConstranst.USER_SUFFIX,ev.uid);
			}
		}

		stormAndEmitConnectionBroken(pipe,collector,ev);
	}

	private void processBroken(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_OFFLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstranst.USER_PREFIX + ev.uid + RedisConstranst.OFFLINE_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_ONLINE,ev,uid,ev.ts) ) {
			// set user status to offline
			pipe.srem(RedisConstranst.ONLINE_USER_KEY,ev.uid);
			pipe.srem(RedisConstranst.APP_PREFIX + ev.app + RedisConstranst.ONLINE_SUFFIX,ev.uid);
		}
	}

	private void processLogout(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_OFFLINE,ev.uid,ev.ts) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstranst.USER_PREFIX + ev.uid + RedisConstranst.OFFLINE_SUFFIX,content);
		}

		if( ! state.isTooOld(TIMELINE_ONLINE,ev,uid,ev.ts) ) {
			// set user status to offline
			pipe.srem(RedisConstranst.ONLINE_USER_KEY,ev.uid);
			pipe.srem(RedisConstranst.APP_PREFIX + ev.app + RedisConstranst.ONLINE_SUFFIX,ev.uid);
		}
	}

	private void stormAndEmitConnectionBroken(Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		String content = state.getGson().toJson(ev);
		pipe.lpush(RedisConstranst.BROKEN_LIST_KEY,content);
		pipe.ltrim(RedisConstranst.BROKEN_LIST_KEY,0,RedisConstranst.BROKEN_LIST_MAX_SIZE);
		collector.emit(new Values(content));
	}
}

