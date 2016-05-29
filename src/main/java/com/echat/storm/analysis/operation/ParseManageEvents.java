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

public class ParseManageEvents extends BaseFunction {
	private static final Logger log = LoggerFactory.getLogger(ParseManageEvents.class);
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
			if( TopologyConstant.EVENT_DISPATCH.equals(ev) ) {
				values = processDispatch(content);
			} else if( TopologyConstant.EVENT_SW_GPS.equals(ev) ) {
				values = processSwAudio(content);
			} else if( TopologyConstant.EVENT_SW_GPS.equals(ev) ) {
				values = processSwGps(content);
			} else if( TopologyConstant.EVENT_TAKE_MIC.equals(ev) ) {
				values = processTakeMic(content);
			} else if( TopologyConstant.EVENT_CREATE_GROUP.equals(ev) ) {
				values = processCreateGroup(content);
			} else if( TopologyConstant.EVENT_RM_GROUP.equals(ev) ) {
				values = processRmGroup(content);
			} else if( TopologyConstant.EVENT_EMPOWER.equals(ev) ) {
				values = processEmpower(content);
			} else if( TopologyConstant.EVENT_EMPOWER_FAILED.equals(ev) ) {
				values = processEmpowerFailed(content);
			} else if( TopologyConstant.EVENT_DEPRIVE.equals(ev) ) {
				values = processDeprive(content);
			} else if( TopologyConstant.EVENT_DEPRIVE_FAILED.equals(ev) ) {
				values = processDepriveFailed(content);
			} else if( TopologyConstant.EVENT_CHANGE_GROUP_NAME.equals(ev) ) {
				values = processChangeGroupName(content);
			} else if( TopologyConstant.EVENT_CHANGE_GROUP_NAME_FAILED.equals(ev) ) {
				values = processChangeGroupNameFailed(content);
			}

			if( values != null ) {
				collector.emit(values);
			}
		}
	}

	private Values processDispatch(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String value = keys.get("gid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,null,value);
		} else {
			return null;
		}
	}

	private Values processSwGps(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");
		String location = keys.get("location");
		String period = keys.get("period");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,location,period);
		} else {
			return null;
		}
	}

	private Values processSwAudio(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");
		String audio = keys.get("audio");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,audio,null);
		} else {
			return null;
		}
	}

	private Values processTakeMic(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,null,null);
		} else {
			return null;
		}
	}
	private Values processCreateGroup(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String value = keys.get("name");

		if( uid != null ) {
			return new Values(uid,target,null,null,null,value);
		} else {
			return null;
		}
	}
	private Values processRmGroup(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String target_got = keys.get("deleted");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,null,null);
		} else {
			return null;
		}
	}
	private Values processEmpower(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("added");
		String value = keys.get("gid");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,null,value);
		} else {
			return null;
		}
	}

	private Values processEmpowerFailed(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String value = keys.get("gid");

		if( uid != null ) {
			return new Values(uid,target,null,null,null,value);
		} else {
			return null;
		}
	}

	private Values processDeprive(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("remove");
		String value = keys.get("gid");

		if( uid != null ) {
			return new Values(uid,target,target_got,null,null,value);
		} else {
			return null;
		}
	}

	private Values processDepriveFailed(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String value = keys.get("gid");

		if( uid != null ) {
			return new Values(uid,target,null,null,null,value);
		} else {
			return null;
		}
	}

	private Values processChangeGroupName(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String value = keys.get("name");

		if( uid != null ) {
			return new Values(uid,target,null,null,null,value);
		} else {
			return null;
		}
	}

	private Values processChangeGroupNameFailed(String content) {
		Map<String,String> keys = reg.getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String value = keys.get("name");

		if( uid != null ) {
			return new Values(uid,target,null,null,null,value);
		} else {
			return null;
		}
	}
}



