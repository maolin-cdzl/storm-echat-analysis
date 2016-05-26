package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;

import com.echat.storm.analysis.FieldsConstrants;
import com.echat.storm.analysis.AnalysisTopologyConstranst;
import com.echat.storm.analysis.utils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Arrays;

public class ParseQueryEvents extends BaseFunction {
	//private static final Logger log = LoggerFactory.getLogger(ParseQueryEvents.class);
	private GetKeyValues reg;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		reg = new GetKeyValues();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//log.info(this.getClass().getName() + ": " + Arrays.toString(tuple.getFields().toList().toArray()));
		final String ev = tuple.getStringByField(FieldsConstrants.EVENT_FIELD);
		final String content = tuple.getStringByField(FieldsConstrants.CONTENT_FIELD);
		if( ev != null && content != null ) {
			Map<String,String> keys = reg.getKeys(content);
			String uid = keys.get("uid");
			String count = keys.get("count");
			String target = keys.get("gids");

			if( count == null ) {
				count = keys.get("gcount");
			}

			if( uid != null ) {
				collector.emit(new Values(uid,count,target));
			}
		}
	}
}

