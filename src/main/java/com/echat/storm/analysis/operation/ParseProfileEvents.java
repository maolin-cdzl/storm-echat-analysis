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

public class ParseProfileEvents extends BaseFunction {
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
			Values values = null;
			if( AnalysisTopologyConstranst.EVENT_CHANGE_NAME.equals(ev) ) {
				values = processChangeName(content);
			} else if( AnalysisTopologyConstranst.EVENT_CHANGE_PWD.equals(ev) ) {
				values = processChangePwd(content);
			} else if( AnalysisTopologyConstranst.EVENT_CHANGE_PWD_FAILED.equals(ev) ) {
				values = processChangePwdFailed(content);
			} else if( AnalysisTopologyConstranst.EVENT_CONTACT_REQ.equals(ev) ) {
				values = processContactReq(content);
			} else if( AnalysisTopologyConstranst.EVENT_CONTACT_REP.equals(ev) ) {
				values = processContactRep(content);
			} else if( AnalysisTopologyConstranst.EVENT_CONTACT_RM.equals(ev) ) {
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


