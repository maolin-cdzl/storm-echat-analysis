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

public class ParseWorkSheetEvents extends BaseFunction {
	private GetKeyValues reg;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		reg = new GetKeyValues();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		final String ev = tuple.getStringByField(FieldsConstrants.EVENT_FIELD);
		final String content = tuple.getStringByField(FieldsConstrants.CONTENT_FIELD);
		if( ev != null && content != null ) {
			Map<String,String> keys = reg.getKeys(content);
			String uid = keys.get("uid");
			String target = keys.get("target");
			String count = keys.get("msgcount");

			if( uid != null ) {
				collector.emit(new Values(uid,target,count));
			}
		}
	}
}


