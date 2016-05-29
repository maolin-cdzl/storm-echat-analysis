package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;

import com.echat.storm.analysis.constant.*;

public class GetEvent extends BaseFunction {
	//take care of the order
	private static final String[][] LOG_PREFIX_TO_EVENT = {
		{"GET MIC",TopologyConstant.EVENT_GET_MIC},
		{"RELEASE MIC",TopologyConstant.EVENT_RELEASE_MIC},
		{"DENT MIC",TopologyConstant.EVENT_DENT_MIC},
		{"LOSTMIC REPLACE",TopologyConstant.EVENT_LOSTMIC_REPLACE},
		{"LOSTMIC AUTO",TopologyConstant.EVENT_LOSTMIC_AUTO},
		{"RELOGIN",TopologyConstant.EVENT_RELOGIN},
		{"LOGIN FAILED",TopologyConstant.EVENT_LOGIN_FAILED},
		{"LOGIN",TopologyConstant.EVENT_LOGIN},
		{"LOGOUT BROKEN",TopologyConstant.EVENT_BROKEN},
		{"LOGOUT",TopologyConstant.EVENT_LOGOUT},
		{"QUERY MEMBERS",TopologyConstant.EVENT_QUERY_MEMBERS},
		{"QUERY GROUP",TopologyConstant.EVENT_QUERY_GROUP},
		{"QUERY CONTACTS",TopologyConstant.EVENT_QUERY_CONTACT},
		{"QUERY DEPARTMENT",TopologyConstant.EVENT_QUERY_DEPARTMENT},
		{"QUERY ENTERPRISE GROUP",TopologyConstant.EVENT_QUERY_ENTERPISE_GROUP},
		{"QUERY USER",TopologyConstant.EVENT_QUERY_USER},
		{"JOIN GROUP",TopologyConstant.EVENT_JOIN_GROUP},
		{"LEAVE GROUP",TopologyConstant.EVENT_LEAVE_GROUP},
		{"CALL",TopologyConstant.EVENT_CALL},
		{"QUICKDIAL",TopologyConstant.EVENT_QUICKDIAL},
		{"CHANGE NAME",TopologyConstant.EVENT_CHANGE_NAME},
		{"CHANGE PWD FAULT",TopologyConstant.EVENT_CHANGE_PWD_FAILED},
		{"CHANGE PWD",TopologyConstant.EVENT_CHANGE_PWD},
		{"CONTACT MAKE",TopologyConstant.EVENT_CONTACT_REQ},
		{"CONTACT RESPONSE",TopologyConstant.EVENT_CONTACT_REP},
		{"CONTACT REMOVE",TopologyConstant.EVENT_CONTACT_RM},
		{"DISPATCH",TopologyConstant.EVENT_DISPATCH},
		{"CONFIG",TopologyConstant.EVENT_CONFIG},
		{"TAKE MIC",TopologyConstant.EVENT_TAKE_MIC},
		{"CREATE GROUP",TopologyConstant.EVENT_CREATE_GROUP},
		{"DELETE GROUP",TopologyConstant.EVENT_RM_GROUP},
		{"EMPOWER FAULT",TopologyConstant.EVENT_EMPOWER_FAILED},
		{"EMPOWER",TopologyConstant.EVENT_EMPOWER},
		{"DEPRIVE FAULT",TopologyConstant.EVENT_DEPRIVE_FAILED},
		{"DEPRIVE",TopologyConstant.EVENT_DEPRIVE},
		{"MODIFY GROUP FAULT",TopologyConstant.EVENT_CHANGE_GROUP_NAME_FAILED},
		{"MODIFY GROUP",TopologyConstant.EVENT_CHANGE_GROUP_NAME},
		{"POST WORKSHEET",TopologyConstant.EVENT_WORKSHEET_POST}
	};

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		final String content = tuple.getStringByField(FieldConstant.CONTENT_FIELD);
		final String ev = getEvent(content);
		if( ev != null ) {
			collector.emit(new Values(ev));
		}
	}

	private String getEvent(String content) {
		for(int i=0; i < LOG_PREFIX_TO_EVENT.length; i++) {
			if( content.startsWith(LOG_PREFIX_TO_EVENT[i][0]) ) {
				// split CONFIG event to audio/location sw event
				if( LOG_PREFIX_TO_EVENT[i][1].equals(TopologyConstant.EVENT_CONFIG) ) {
					if( content.contains("audio") ) {
						return TopologyConstant.EVENT_SW_AUDIO;
					} else if( content.contains("location") ) {
						return TopologyConstant.EVENT_SW_GPS;
					}
				} else {
					return LOG_PREFIX_TO_EVENT[i][1];
				}
			}
		}
		return null;
	}
}

