package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.utils.*;

import java.util.Map;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseLoginFailedEvents extends BaseFunction {
	private static final Logger log = LoggerFactory.getLogger(ParseLoginFailedEvents.class);
	private GetKeyValues reg;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		reg = new GetKeyValues();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if( ! tuple.contains(FieldConstant.EVENT_FIELD) || !tuple.contains(FieldConstant.CONTENT_FIELD) ) {
			log.warn("Can not found all need fields in: " + Arrays.toString(tuple.getFields().toList().toArray()));
			return;
		}
		final String ev = tuple.getStringByField(FieldConstant.EVENT_FIELD);
		final String content = tuple.getStringByField(FieldConstant.CONTENT_FIELD);
		if( ev != null && content != null ) {
			Map<String,String> keys = reg.getKeys(content);
			String uid = keys.get("uid");
			if( uid == null ) {
				uid = keys.get("account");
			}
			String ctx = keys.get("context");
			String ip = keys.get("address");
			String version = keys.get("version");
			String device = keys.get("device");
			String meid = keys.get("meid");
			String esn = keys.get("esn");
			String imsi = keys.get("imsi");
			String expect_payload = keys.get("expect_payload");
			String reason = keys.get("result");

			String devid = null;
			if( meid != null ) {
				devid = "meid-" + meid;
			} else if( esn != null ) {
				devid = "esn-" + esn;
			}
			if( uid != null ) {
				collector.emit(new Values(reason,uid,ctx,ip,device,devid,version,imsi,expect_payload));
			}
		}
	}
}



