package com.echat.storm.analysis.types;

import java.util.Date;
import java.text.ParseException;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.ITuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.TopologyConstant;

public class GroupEvent {
	public String			server;
	public String			datetime;
	public String			event;
	public String			uid;
	public String			company;
	public String			agent;
	public String			gid;

	// no transaction member
	private transient long timestamp = 0;


	static public Fields newFields() {
		return new Fields(
			FieldConstant.SERVER_FIELD,
			FieldConstant.DATETIME_FIELD,
			FieldConstant.EVENT_FIELD,
			FieldConstant.UID_FIELD,
			FieldConstant.COMPANY_FIELD,
			FieldConstant.AGENT_FIELD,
			FieldConstant.GID_FIELD
		);
	}

	static public GroupEvent fromTuple(ITuple tuple) {
		GroupEvent ev = new GroupEvent();

		ev.server = tuple.getString(0);
		ev.datetime = tuple.getString(1);
		ev.event = tuple.getString(2);
		ev.uid = tuple.getString(3);
		ev.company = tuple.getString(4);
		ev.agent = tuple.getString(5);
		ev.gid = tuple.getString(6);

		return ev;
	}

	public Date getDate() {
		try {
			return DateUtils.parseDate(datetime,TopologyConstant.STD_INPUT_DATETIME_FORMAT);
		} catch( ParseException e) {
			throw new RuntimeException("Bad datetime format: " + datetime);
		}
	}
	public long getTimeStamp() {
		if( timestamp == 0 ) {
			timestamp = getDate().getTime();
		}
		return timestamp;
	}
}
