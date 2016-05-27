package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;

import com.echat.storm.analysis.FieldsConstrants;
import com.echat.storm.analysis.AnalysisTopologyConstranst;
import com.echat.storm.analysis.utils.*;

import java.util.Map;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseCallEvents extends BaseFunction {
	private static final Logger log = LoggerFactory.getLogger(ParseCallEvents.class);
	private GetKeyValues reg = null;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		reg = new GetKeyValues();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if( ! tuple.contains(FieldsConstrants.EVENT_FIELD) || !tuple.contains(FieldsConstrants.CONTENT_FIELD) ) {
			log.warn("Can not found all need fields in: " + Arrays.toString(tuple.getFields().toList().toArray()));
			return;
		}
		final String ev = tuple.getStringByField(FieldsConstrants.EVENT_FIELD);
		final String content = tuple.getStringByField(FieldsConstrants.CONTENT_FIELD);
		if( AnalysisTopologyConstranst.DEBUG ) {
			log.info("Parse event:" + ev + " content:" + content);
		}
		if( ev != null && content != null ) {
			Map<String,String> keys = reg.getKeys(content);
			String uid = keys.get("uid");
			String target = keys.get("targets");
			String called = keys.get("called");

			if( uid != null ) {
				collector.emit(new Values(uid,target,called));
			}
		}
	}
}

