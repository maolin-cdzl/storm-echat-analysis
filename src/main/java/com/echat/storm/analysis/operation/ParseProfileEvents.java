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

public class ParseProfileEvents extends BaseFunction {
	private static final Logger log = LoggerFactory.getLogger(ParseProfileEvents.class);
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
			Values values = null;
			if( TopologyConstant.EVENT_CHANGE_NAME.equals(ev) ) {
				values = processChangeName(content);
			} else if( TopologyConstant.EVENT_CHANGE_PWD.equals(ev) ) {
				values = processChangePwd(content);
			} else if( TopologyConstant.EVENT_CHANGE_PWD_FAILED.equals(ev) ) {
				values = processChangePwdFailed(content);
			} else if( TopologyConstant.EVENT_CONTACT_REQ.equals(ev) ) {
				values = processContactReq(content);
			} else if( TopologyConstant.EVENT_CONTACT_REP.equals(ev) ) {
				values = processContactRep(content);
			} else if( TopologyConstant.EVENT_CONTACT_RM.equals(ev) ) {
				values = processContactRM(content);
			}

			if( values != null ) {
				collector.emit(values);
			}
		}
	}

	private Values processChangeName(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target_got = keys.get("name");

		if( uid != null ) {
			return new Values(uid,null,target_got,null);
		} else {
			return null;
		}
	}

	private Values processChangePwd(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("oldpwd");
		String target_got = keys.get("newpwd");

		if( uid != null ) {
			return new Values(uid,target,target_got,null);
		} else {
			return null;
		}
	}
	private Values processChangePwdFailed(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("oldpwd");
		String target_dent = keys.get("newpwd");

		if( uid != null ) {
			return new Values(uid,target,null,target_dent);
		} else {
			return null;
		}
	}
	private Values processContactReq(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("requested");

		if( uid != null ) {
			return new Values(uid,target,target_got,null);
		} else {
			return null;
		}
	}
	private Values processContactRep(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("accept");
		String target_got = keys.get("paired");
		String target_dent = keys.get("refuse");

		if( uid != null ) {
			return new Values(uid,target,target_got,target_dent);
		} else {
			return null;
		}
	}
	private Values processContactRM(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");

		if( uid != null ) {
			return new Values(uid,target,null,null);
		} else {
			return null;
		}
	}
}


