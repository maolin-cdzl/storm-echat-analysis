package com.echat.storm.analysis;

import backtype.storm.tuple.Tuple;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import redis.clients.jedis.JedisCommands;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsPersist extends AbstractRedisBolt {
	private static final Logger log = LoggerFactory.getLogger(StatisticsPersist.class);

	private RedisDataTypeDescription description;

	public StatisticsPersist(JedisPoolConfig config) {
            super(config);
        }

	public StatisticsPersist(JedisClusterConfig config) {
		super(config);
	}

	@Override
	public void execute(Tuple input) {
		JedisCommands jedisCommands = null;
		try {
			String app = input.getStringByField(FieldsConstrants.APP_FIELD);
			String report = input.getStringByField(FieldsConstrants.APP_LOAD_FIELD);

			if( app != null && report != null ) {
				jedisCommands = getInstance();
				final String key = AnalysisTopologyConstranst.KEY_APP_LOAD_PREFIX + app;
				long len = jedisCommands.rpush(key,report);
				if( len > AnalysisTopologyConstranst.MAX_APP_LOAD_LENGTH ) {
					jedisCommands.ltrim(key,-AnalysisTopologyConstranst.MAX_APP_LOAD_LENGTH,-1);
				}
			} else {
				log.warn("app or app-load fields missing");
			}
		} finally {
			if (jedisCommands != null) {
				returnInstance(jedisCommands);
			}
			this.collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}

