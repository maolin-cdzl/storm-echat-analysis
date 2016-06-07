package com.echat.storm.analysis.state;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.State;
import storm.trident.state.BaseStateUpdater;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;
import com.echat.storm.analysis.utils.*;

public class GroupUpdater extends BaseStateUpdater<BaseState> {
	private class MemberCountReponse {
		public String					gid;
		public Response<Long>			count;
		public Response<Set<String>>	servers;
	}

	private static final Logger logger = LoggerFactory.getLogger(GroupUpdater.class);
	
	private long			lastCheck;
	private HashSet<String> groupJoined;
	private HashSet<String> groupLeft;

	static public String[] getGroupEvents() {
		return new String[] {
			EventConstant.EVENT_JOIN_GROUP,
			EventConstant.EVENT_LEAVE_GROUP
		};
	}
	@Override
	public void prepare(Map conf,TridentOperationContext context) {
		groupJoined= new HashSet<String>();
		groupLeft = new HashSet<String>();
		lastCheck = 0;
	}

	@Override
	public void updateState(BaseState state, List<TridentTuple> inputs,TridentCollector collector) {
		logger.info("updateState, input tuple count: " + inputs.size());

		Jedis jedis = null;
		try {
			jedis = state.getJedis();
			Pipeline pipe = jedis.pipelined();
			for(TridentTuple tuple : inputs) {
				GroupEvent ev = GroupEvent.fromTuple(tuple);
				if( EventConstant.EVENT_JOIN_GROUP.equals(ev.event) ) {
					processJoin(state,pipe,collector,ev);
				} else if( EventConstant.EVENT_LEAVE_GROUP.equals(ev.event) ) {
					processLeave(state,pipe,collector,ev);
				} else {
					logger.error("Unknown event: " + ev.event);
				}
			}
			pipe.sync();

			final long now = System.currentTimeMillis();
			if( now - lastCheck > 60 * 1000 || groupLeft.size() > 1000 ) {
				lastCheck = now;
				groupJoined.clear();

				List<MemberCountReponse> ress = new LinkedList<MemberCountReponse>();
				for(String gid : groupLeft) {
					MemberCountReponse mc = new MemberCountReponse();
					mc.gid = gid;
					mc.count = pipe.scard(RedisConstant.GROUP_PREFIX + gid + RedisConstant.USER_SUFFIX);
					mc.servers = pipe.smembers(RedisConstant.GROUP_PREFIX + gid + RedisConstant.SERVER_SUFFIX);
					ress.add(mc);
				}
				pipe.sync();
				
				for(MemberCountReponse mc : ress) {
					if( mc.count.get() == 0 ) {
						// group has no members in
						Set<String> servers = mc.servers.get();
						
						pipe.srem(RedisConstant.ONLINE_GROUP_KEY,mc.gid);
						pipe.del(RedisConstant.GROUP_PREFIX + mc.gid + RedisConstant.SERVER_SUFFIX);

						for(String s : servers) {
							pipe.srem(RedisConstant.SERVER_PREFIX + s + RedisConstant.GROUP_SUFFIX,mc.gid);
						}
					}
				}
				groupLeft.clear();
				pipe.sync();
			}
		} finally {
			if( jedis != null ) {
				state.returnJedis(jedis);
			}
		}
	}

	private void processJoin(BaseState state,Pipeline pipe,TridentCollector collector,GroupEvent ev) {
		if( state.updateTimeline(ev.gid,ev.uid,ev.getTimeStamp()) ) {
			// set user join group
			pipe.set(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX,ev.gid);
			pipe.sadd(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.USER_SUFFIX,ev.uid);

			if( ! groupJoined.contains(ev.gid) ) {
				pipe.sadd(RedisConstant.ONLINE_GROUP_KEY,ev.gid); 
				groupJoined.add(ev.gid);
			}
			final String gsKey = ev.gid + ev.server;
			if( ! groupJoined.contains(gsKey) ) {
				pipe.sadd(RedisConstant.SERVER_PREFIX + ev.server + RedisConstant.GROUP_SUFFIX,ev.gid);
				pipe.sadd(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.SERVER_SUFFIX,ev.server);
				groupJoined.add(gsKey);
			}
		}
	}

	private void processLeave(BaseState state,Pipeline pipe,TridentCollector collector,GroupEvent ev) {
		if( state.updateTimeline(ev.gid,ev.uid,ev.getTimeStamp()) ) {
			// set user leave group
			pipe.del(RedisConstant.USER_PREFIX + ev.uid + RedisConstant.GROUP_SUFFIX);
			pipe.srem(RedisConstant.GROUP_PREFIX + ev.gid + RedisConstant.USER_SUFFIX,ev.uid);

			groupLeft.add(ev.gid);
		}
	}
}

