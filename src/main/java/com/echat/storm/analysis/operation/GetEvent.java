package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;

import com.echat.storm.analysis.FieldsConstrants;
import com.echat.storm.analysis.AnalysisTopologyConstranst;

public class GetEvent extends BaseFunction {
	//take care of the order
	private static final String[][] LOG_PREFIX_TO_EVENT = {
		{"GET MIC",AnalysisTopologyConstranst.EVENT_GET_MIC},
		{"RELEASE MIC",AnalysisTopologyConstranst.EVENT_RELEASE_MIC},
		{"DENT MIC",AnalysisTopologyConstranst.EVENT_DENT_MIC},
		{"LOSTMIC REPLACE",AnalysisTopologyConstranst.EVENT_LOSTMIC_REPLACE},
		{"LOSTMIC AUTO",AnalysisTopologyConstranst.EVENT_LOSTMIC_AUTO},
		{"RELOGIN",AnalysisTopologyConstranst.EVENT_RELOGIN},
		{"LOGIN FAILED",AnalysisTopologyConstranst.EVENT_LOGIN_FAILED},
		{"LOGIN",AnalysisTopologyConstranst.EVENT_LOGIN},
		{"LOGOUT BROKEN",AnalysisTopologyConstranst.EVENT_BROKEN},
		{"LOGOUT",AnalysisTopologyConstranst.EVENT_LOGOUT},
		{"QUERY MEMBERS",AnalysisTopologyConstranst.EVENT_QUERY_MEMBERS},
		{"QUERY GROUP",AnalysisTopologyConstranst.EVENT_QUERY_GROUP},
		{"QUERY CONTACTS",AnalysisTopologyConstranst.EVENT_QUERY_CONTACT},
		{"QUERY DEPARTMENT",AnalysisTopologyConstranst.EVENT_QUERY_DEPARTMENT},
		{"QUERY ENTERPRISE GROUP",AnalysisTopologyConstranst.EVENT_QUERY_ENTERPISE_GROUP},
		{"QUERY USER",AnalysisTopologyConstranst.EVENT_QUERY_USER},
		{"JOIN GROUP",AnalysisTopologyConstranst.EVENT_JOIN_GROUP},
		{"LEAVE GROUP",AnalysisTopologyConstranst.EVENT_LEAVE_GROUP},
		{"CALL",AnalysisTopologyConstranst.EVENT_CALL},
		{"QUICKDIAL",AnalysisTopologyConstranst.EVENT_QUICKDIAL},
		{"CHANGE NAME",AnalysisTopologyConstranst.EVENT_CHANGE_NAME},
		{"CHANGE PWD FAULT",AnalysisTopologyConstranst.EVENT_CHANGE_PWD_FAILED},
		{"CHANGE PWD",AnalysisTopologyConstranst.EVENT_CHANGE_PWD},
		{"CONTACT MAKE",AnalysisTopologyConstranst.EVENT_CONTACT_REQ},
		{"CONTACT RESPONSE",AnalysisTopologyConstranst.EVENT_CONTACT_REP},
		{"CONTACT REMOVE",AnalysisTopologyConstranst.EVENT_CONTACT_RM},
		{"DISPATCH",AnalysisTopologyConstranst.EVENT_DISPATCH},
		{"CONFIG",AnalysisTopologyConstranst.EVENT_CONFIG},
		{"TAKE MIC",AnalysisTopologyConstranst.EVENT_TAKE_MIC},
		{"CREATE GROUP",AnalysisTopologyConstranst.EVENT_CREATE_GROUP},
		{"DELETE GROUP",AnalysisTopologyConstranst.EVENT_RM_GROUP},
		{"EMPOWER FAULT",AnalysisTopologyConstranst.EVENT_EMPOWER_FAILED},
		{"EMPOWER",AnalysisTopologyConstranst.EVENT_EMPOWER},
		{"DEPRIVE FAULT",AnalysisTopologyConstranst.EVENT_DEPRIVE_FAILED},
		{"DEPRIVE",AnalysisTopologyConstranst.EVENT_DEPRIVE},
		{"MODIFY GROUP FAULT",AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME_FAILED},
		{"MODIFY GROUP",AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME},
		{"POST WORKSHEET",AnalysisTopologyConstranst.EVENT_WORKSHEET_POST}
	};

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		final String content = tuple.getStringByField(FieldsConstrants.CONTENT_FIELD);
		final String ev = getEvent(content);
		if( ev != null ) {
			collector.emit(new Values(ev));
		}
	}

	private String getEvent(String content) {
		for(int i=0; i < LOG_PREFIX_TO_EVENT.length; i++) {
			if( content.startsWith(LOG_PREFIX_TO_EVENT[i][0]) ) {
				// split CONFIG event to audio/location sw event
				if( LOG_PREFIX_TO_EVENT[i][1].equals(AnalysisTopologyConstranst.EVENT_CONFIG) ) {
					if( content.contains("audio") ) {
						return AnalysisTopologyConstranst.EVENT_SW_AUDIO;
					} else if( content.contains("location") ) {
						return AnalysisTopologyConstranst.EVENT_SW_GPS;
					}
				} else {
					return LOG_PREFIX_TO_EVENT[i][1];
				}
			}
		}
		return null;
	}
}

