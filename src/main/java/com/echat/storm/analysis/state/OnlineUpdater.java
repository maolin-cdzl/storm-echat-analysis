package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
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


	private HashSet<String>							_servers = new HashSet<String>();
	private HashSet<String>							_devices = new HashSet<String>();
	private HashMap<String,List<OnlineEvent>>		_events = new HashMap<String,List<OnlineEvent>>();

	static public String[] getOnlineEvents() {
		return new String[] {
			EventConstant.EVENT_LOGIN,
			EventConstant.EVENT_RELOGIN,
			EventConstant.EVENT_BROKEN,
			EventConstant.EVENT_LOGOUT
		};
	}

	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		log.info("updateState, input tuple count: " + inputs.size());

		HashSet<String> newServer = new HashSet<String>();
		HashSet<String> newDevice = new HashSet<String>();

		for(TridentTuple tuple : inputs) {
			OnlineEvent ev = OnlineEvent.fromTuple(tuple);
			if( ev != null ) {
				put(ev);

				if( ! _servers.contains(ev.server) ) {
					newServer.add(ev.server);
					_servers.add(ev.server);
				}
				if( ev.device != null ) {
					if( ! _devices.contains(ev.device) ) {
						newDevice.add(ev.device);
						_devices.add(ev.device);
					}
				}
			}
		}

		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.pipelined();

			if( ! newServer.isEmpty() ) {
				for(String server: newServer) {
					pipe.sadd(RedisConstant.SERVER_SET_KEY,server);
				}
				for(String device: newDevice) {
					pipe.sadd(RedisConstant.DEVICE_SET_KEY,device);
				}
				pipe.sync();
			}
			for (List<OnlineEvent> l : _events.values() ) {
				for(OnlineEvent ev : l) {
					if( EventConstant.EVENT_LOGIN.equals(ev.event) ) {
						processLogin(state,pipe,collector,ev);
					} else if( EventConstant.EVENT_RELOGIN.equals(ev.event) ) {
						processRelogin(state,pipe,collector,ev);
					} else if( EventConstant.EVENT_BROKEN.equals(ev.event) ) {
						processBroken(state,pipe,collector,ev);
					} else if( EventConstant.EVENT_LOGOUT.equals(ev.event) ) {
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
			l = new SortedLinkedList<OnlineEvent>(OnlineEvent.dateComparator());
			_events.put(ev.uid,l);
		}
		l.add(ev);
	}

	private void processLogin(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_ONLINE,ev.uid,ev.getTimeStamp()) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX,content);
			if( !state.isTooOld(TIMELINE_OFFLINE,ev.uid,ev.getTimeStamp()) ) {
				setUserOnline(state,pipe,collector,ev);

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
						if( EventConstant.EVENT_BROKEN.equals(ev.event) ) {
							if( ev.getTimeStamp() > lastLogout.getTimeStamp() ) {
							   	long offtime = TimeUnit.MILLISECONDS.toSeconds(ev.getTimeStamp() - lastLogout.getTimeStamp());
								if( offtime < 300 ) {
									collector.emit(BrokenEvent.create(ev,offtime).toValues());
								}
							}
						}
					}
				}
			} else {
				log.warn("Stale Login events,datetime: " + 
						ev.datetime + 
						",last logout datetime: " + 
						DateFormatUtils.format(new Date(state.getTimeline(TIMELINE_OFFLINE,ev.uid)),TopologyConstant.STD_DATETIME_FORMAT));
			}
		} else {
			log.warn("Stale Login events,datetime: " + 
					ev.datetime +
					",last login datetime: " + 
						DateFormatUtils.format(new Date(state.getTimeline(TIMELINE_ONLINE,ev.uid)),TopologyConstant.STD_DATETIME_FORMAT));
		}
	}

	private void processRelogin(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.timelineBefore(ev.uid,TIMELINE_ONLINE,TIMELINE_OFFLINE) &&
			state.updateTimeline(TIMELINE_ONLINE,ev.uid,ev.getTimeStamp())  &&
			!state.isTooOld(TIMELINE_OFFLINE,ev.uid,ev.getTimeStamp()) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX,content);
			setUserOnline(state,pipe,collector,ev);
		}

		collector.emit(BrokenEvent.create(ev,0).toValues());
	}

	private void processBroken(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_OFFLINE,ev.uid,ev.getTimeStamp()) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGOUT_SUFFIX,content);

			if( ! state.isTooOld(TIMELINE_ONLINE,ev.uid,ev.getTimeStamp()) ) {
				setUserOffline(state,pipe,collector,ev);
			}
		}
	}

	private void processLogout(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		if( state.updateTimeline(TIMELINE_OFFLINE,ev.uid,ev.getTimeStamp()) ) {
			String content = state.getGson().toJson(ev);
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGOUT_SUFFIX,content);
			if( ! state.isTooOld(TIMELINE_ONLINE,ev.uid,ev.getTimeStamp()) ) {
				setUserOffline(state,pipe,collector,ev);
			}
		}
	}

	private void setUserOnline(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.STATE_SUFFIX,RedisConstant.STATE_ONLINE);
		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.SERVER_SUFFIX,ev.server);
		pipe.sadd(RedisConstant.ONLINE_USER_KEY,ev.uid);

		pipe.sadd(RedisConstant.SERVER_PREFIX + ev.server + RedisConstant.USER_SUFFIX,ev.uid);
		pipe.sadd(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.SERVER_SET_SUFFIX,ev.server);

		if( ev.device != null ) {
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SUFFIX,ev.device);
			pipe.sadd(RedisConstant.DEVICE_PREFIX + ev.device + RedisConstant.USER_SUFFIX,ev.uid);
			pipe.sadd(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SET_SUFFIX,ev.device);
		}

	}

	private void setUserOffline(BaseState state,Pipeline pipe,TridentCollector collector,OnlineEvent ev) {
		pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.STATE_SUFFIX,RedisConstant.STATE_OFFLINE);
		pipe.srem(RedisConstant.ONLINE_USER_KEY,ev.uid);
		pipe.srem(RedisConstant.SERVER_PREFIX + ev.server + RedisConstant.USER_SUFFIX,ev.uid);

		String device = null;
		String lastLoginJson = null;

		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			device = jedis.get(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.DEVICE_SUFFIX);
			lastLoginJson = jedis.get(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.LAST_LOGIN_SUFFIX);
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}

		if( device != null ) {
			pipe.srem(RedisConstant.DEVICE_PREFIX + device + RedisConstant.USER_SUFFIX,ev.uid);
		}

		if( lastLoginJson != null ) {
			OnlineEvent lastLogin = state.getGson().fromJson(lastLoginJson,OnlineEvent.class);
			if( lastLogin != null ) {
				OnlineSession session = OnlineSession.create(lastLogin,ev);
				if( session != null ) {
					String sessionJson = state.getGson().toJson(session);
					pipe.lpush(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.SESSION_LIST_SUFFIX,sessionJson);
					pipe.ltrim(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.SESSION_LIST_SUFFIX,0,100);
				}
			}

		}
	}
}

