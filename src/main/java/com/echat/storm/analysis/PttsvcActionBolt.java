package com.echat.storm.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

public class PttsvcActionBolt extends BaseRichBolt {
	private static final Logger log = LoggerFactory.getLogger(PttsvcActionBolt.class);

    private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			String app = input.getStringByField(FieldsConstrants.APP_FIELD);
			String datetime = input.getStringByField(FieldsConstrants.DATETIME_FIELD);
			String level = input.getStringByField(FieldsConstrants.LEVEL_FIELD);
			String content = input.getStringByField(FieldsConstrants.CONTENT_FIELD);

			log.debug("PttsvcActionBolt : " + app + "\t" + datetime + "\t" + level + "\t" + content);
		} catch( IllegalArgumentException e ) {
			log.warn(e.getMessage());
		}

		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("word", "count"));         
	}
}

