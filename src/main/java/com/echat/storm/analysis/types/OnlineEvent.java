package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.Arrays;
import java.util.Comparator;
import java.text.ParseException;

import storm.trident.tuple.TridentTuple;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.DateFormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;

public class OnlineEvent {
	private static final Logger log = LoggerFactory.getLogger(OnlineEvent.class);

	public String server;
	public Long ts;
	public Date date;
	public String event;
	public String uid;
	public String ctx;
	public String ip;
	public String device;
	public String devid;
	public String version;
	public String imsi;
	public String expect_pt;

	static public Comparator<OnlineEvent> tsComparator() {
		return new Comparator<OnlineEvent>() {
			@Override
			public int compare(OnlineEvent e1,OnlineEvent e2) {
				if( e1.ts == e2.ts ) {
					return 0;
				} else if( e1.ts < e2.ts ) {
					return -1;
				} else {
					return 1;
				}
			}

			@Override
			public boolean equals(Object other) {
				return (other.getClass().isAssignableFrom(this.getClass()));
			}
		};
	}

	static public OnlineEvent fromTuple(TridentTuple tuple) {
		OnlineEvent ev = new OnlineEvent();

		try {
			ev.date = DateUtils.parseDate(tuple.getStringByField(FieldConstant.DATETIME_FIELD),TopologyConstant.INPUT_DATETIME_FORMAT);
		} catch( ParseException e) {
			log.error("Bad datetime format: " + tuple.getStringByField(FieldConstant.DATETIME_FIELD));
			return null;
		}
		ev.server = tuple.getStringByField(FieldConstant.SERVER_FIELD);
		ev.ts = tuple.getLongByField(FieldConstant.TIMESTAMP_FIELD);
		ev.event = tuple.getStringByField(FieldConstant.EVENT_FIELD);
		ev.uid = tuple.getStringByField(FieldConstant.UID_FIELD);
		
		if( tuple.contains(FieldConstant.CTX_FIELD) ) {
			ev.ctx = tuple.getStringByField(FieldConstant.CTX_FIELD);
		}
		if( tuple.contains(FieldConstant.IP_FIELD) ) {
			ev.ip = tuple.getStringByField(FieldConstant.IP_FIELD);
		}
		if( tuple.contains(FieldConstant.DEVICE_FIELD) ) {
			ev.device = tuple.getStringByField(FieldConstant.DEVICE_FIELD);
		}
		if( tuple.contains(FieldConstant.DEVICE_ID_FIELD) ) {
			ev.devid = tuple.getStringByField(FieldConstant.DEVICE_ID_FIELD);
		}
		if( tuple.contains(FieldConstant.VERSION_FIELD) ) {
			ev.version = tuple.getStringByField(FieldConstant.VERSION_FIELD);
		}
		if( tuple.contains(FieldConstant.IMSI_FIELD) ) {
			ev.imsi = tuple.getStringByField(FieldConstant.IMSI_FIELD);
		}
		if( tuple.contains(FieldConstant.EXPECT_PAYLOAD_FIELD) ) {
			ev.expect_pt = tuple.getStringByField(FieldConstant.EXPECT_PAYLOAD_FIELD);
		}

		return ev;
	}
}
