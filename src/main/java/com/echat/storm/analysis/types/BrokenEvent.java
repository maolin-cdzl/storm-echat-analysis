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

public class BrokenEvent {
	//private static final Logger logger = LoggerFactory.getLogger(BrokenEvent.class);

	public String			server;
	public String			datetime;
	public String			uid;
	public String			company;
	public String			agent;
	public Long				offtime;
	public String			ctx;
	public String			ip;
	public String			device;
	public String			devid;
	public String			version;
	public String			imsi;

	// no transaction member
	private transient long timestamp = 0;

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

	static public Fields newFields() {
		return new Fields(
			FieldConstant.SERVER_FIELD,
			FieldConstant.DATETIME_FIELD,
			FieldConstant.UID_FIELD,
			FieldConstant.COMPANY_FIELD,
			FieldConstant.AGENT_FIELD,
			FieldConstant.OFFLINE_TIME_FIELD,
			FieldConstant.CTX_FIELD,
			FieldConstant.IP_FIELD,
			FieldConstant.DEVICE_FIELD,
			FieldConstant.DEVICE_ID_FIELD,
			FieldConstant.VERSION_FIELD,
			FieldConstant.IMSI_FIELD
		);
	}

	static public BrokenEvent create(OnlineEvent login,long offtime) {
		BrokenEvent broken = new BrokenEvent();
		broken.server = login.server;
		broken.datetime = login.datetime;
		broken.uid = login.uid;
		broken.company = login.company;
		broken.agent = login.agent;
		broken.offtime = offtime;
		broken.ctx = login.ctx;
		broken.ip = login.ip;
		broken.device = login.device;
		broken.devid = login.devid;
		broken.version = login.version;
		broken.imsi = login.imsi;

		return broken;
	}

	static public BrokenEvent fromTuple(ITuple tuple) {
		BrokenEvent broken = new BrokenEvent();

		broken.server = tuple.getString(0);
		broken.datetime = tuple.getString(1);
		broken.uid = tuple.getString(2);
		broken.company = tuple.getString(3);
		broken.agent = tuple.getString(4);
		broken.offtime = tuple.getLong(5);
		broken.ctx = tuple.getString(6);
		broken.ip = tuple.getString(7);
		broken.device = tuple.getString(8);
		broken.devid = tuple.getString(9);
		broken.version = tuple.getString(10);
		broken.imsi = tuple.getString(11);

		return broken;
	}

	public Values toValues() {
		return new Values(
			server,
			datetime,
			uid,
			company,
			agent,
			offtime,
			ctx,
			ip,
			device,
			devid,
			version,
			imsi
		);
	}
}

