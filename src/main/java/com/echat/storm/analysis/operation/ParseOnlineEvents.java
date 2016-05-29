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

public class ParseOnlineEvents extends BaseFunction {
	private static final Logger log = LoggerFactory.getLogger(ParseOnlineEvents.class);
	private GetKeyValues reg;

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
		if( ev != null && content != null ) {
			Values values = null;
			if( AnalysisTopologyConstranst.EVENT_LOGIN.equals(ev) ) {
				values = processLogin(content);
			} else if( AnalysisTopologyConstranst.EVENT_RELOGIN.equals(ev) ) {
				values = processRelogin(content);
			} else if( AnalysisTopologyConstranst.EVENT_BROKEN.equals(ev) ) {
				values = processBroken(content);
			} else if( AnalysisTopologyConstranst.EVENT_LOGOUT.equals(ev) ) {
				values = processLogout(content);
			}

			if( values != null ) {
				collector.emit(values);
			}
		}
	}

	private Values processLogin(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String ctx = keys.get("context");
		String ip = keys.get("address");
		String version = keys.get("version");
		String device = keys.get("device");
		String meid = keys.get("meid");
		String esn = keys.get("esn");
		String imsi = keys.get("imsi");
		String expect_payload = keys.get("expect_payload");

		String devid = null;
		if( meid != null ) {
			devid = "meid-" + meid;
		} else if( esn != null ) {
			devid = "esn-" + esn;
		}
		if( uid != null ) {
			return new Values(uid,ctx,ip,device,devid,version,imsi,expect_payload,null);
		} else {
			return null;
		}
	}

	private Values processRelogin(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String ctx = keys.get("context");
		String ip = keys.get("address");
		String version = keys.get("version");
		String device = keys.get("device");
		String meid = keys.get("meid");
		String esn = keys.get("esn");
		String imsi = keys.get("imsi");
		String expect_payload = keys.get("expect_payload");

		String devid = null;
		if( meid != null ) {
			devid = "meid-" + meid;
		} else if( esn != null ) {
			devid = "esn-" + esn;
		}
		if( uid != null ) {
			return new Values(uid,ctx,ip,device,devid,version,imsi,expect_payload);
		} else {
			return null;
		}
	}

	private Values processLogout(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String ip = keys.get("address");
		if( uid != null ) {
			return new Values(uid,null,ip,null,null,null,null,null);
		} else {
			return null;
		}
	}

	private Values processBroken(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String ip = keys.get("address");

		if( uid != null ) {
			return new Values(uid,null,ip,null,null,null,null,null);
		} else {
			return null;
		}
	}

}

