package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.Arrays;
import java.util.Comparator;
import java.text.ParseException;

import backtype.storm.tuple.Fields;
import storm.trident.tuple.TridentTuple;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;

public class OnlineEvent {
	private static final Logger log = LoggerFactory.getLogger(OnlineEvent.class);

	public String server;
	public String datetime;
	public String event;
	public String uid;
	public String company;
	public String agent;
	public String ctx;
	public String ip;
	public String device;
	public String devid;
	public String version;
	public String imsi;
	public String expect_pt;

	// no transaction member
	private transient long timestamp = 0;

	static public Comparator<OnlineEvent> dateComparator() {
		return new Comparator<OnlineEvent>() {
			@Override
			public int compare(OnlineEvent e1,OnlineEvent e2) {
				return e1.datetime.compareTo(e2.datetime);
			}

			@Override
			public boolean equals(Object other) {
				return (other.getClass().isAssignableFrom(this.getClass()));
			}
		};
	}

	static public Fields newFields() {
		return new Fields(
			FieldConstant.SERVER_FIELD,
			FieldConstant.DATETIME_FIELD,
			FieldConstant.EVENT_FIELD,
			FieldConstant.UID_FIELD,
			FieldConstant.COMPANY_FIELD,
			FieldConstant.AGENT_FIELD,
			FieldConstant.CTX_FIELD,
			FieldConstant.IP_FIELD,
			FieldConstant.DEVICE_FIELD,
			FieldConstant.DEVICE_ID_FIELD,
			FieldConstant.VERSION_FIELD,
			FieldConstant.IMSI_FIELD,
			FieldConstant.EXPECT_PAYLOAD_FIELD
		);
	}

	static public OnlineEvent fromTuple(TridentTuple tuple) {
		OnlineEvent ev = new OnlineEvent();

		ev.server = tuple.getString(0);
		ev.datetime = tuple.getString(1);
		ev.event = tuple.getString(2);
		ev.uid = tuple.getString(3);
		ev.company = tuple.getString(4);
		ev.agent = tuple.getString(5);
		ev.ctx = tuple.getString(6);
		ev.ip = tuple.getString(7);
		ev.device = tuple.getString(8);
		ev.devid = tuple.getString(9);
		ev.version = tuple.getString(10);
		ev.imsi = tuple.getString(11);
		ev.expect_pt = tuple.getString(12);

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
