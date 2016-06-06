package com.echat.storm.analysis.types;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Tuple;
import com.echat.storm.analysis.constant.FieldConstant;

public class PttSvcLog {
	static public Fields newFields() {
		return new Fields(
				FieldConstant.SERVER_FIELD,
				FieldConstant.DATETIME_FIELD,
				FieldConstant.TIMESTAMP_FIELD,
				FieldConstant.LEVEL_FIELD,
				FieldConstant.CONTENT_FIELD,
				FieldConstant.EVENT_FIELD,
				FieldConstant.UID_FIELD,
				FieldConstant.GID_FIELD,
				FieldConstant.COMPANY_FIELD,
				FieldConstant.AGENT_FIELD,
				FieldConstant.RESULT_FIELD,
				FieldConstant.CTX_FIELD,
				FieldConstant.IP_FIELD,
				FieldConstant.DEVICE_FIELD,
				FieldConstant.DEVICE_ID_FIELD,
				FieldConstant.VERSION_FIELD,
				FieldConstant.IMSI_FIELD,
				FieldConstant.EXPECT_PAYLOAD_FIELD,
				FieldConstant.TARGET_FIELD,
				FieldConstant.TARGET_GOT_FIELD,
				FieldConstant.TARGET_DENT_FIELD,
				FieldConstant.COUNT_FIELD,
				FieldConstant.SW_FIELD,
				FieldConstant.VALUE_FIELD
					);
	}
	
	static public PttSvcLog fromTuple(Tuple tuple) {
		PttSvcLog log = new PttSvcLog();
		log.server = tuple.getStringByField(FieldConstant.SERVER_FIELD);
		log.datetime = tuple.getStringByField(FieldConstant.DATETIME_FIELD);
		log.timestamp = tuple.getLongByField(FieldConstant.TIMESTAMP_FIELD);
		log.level = tuple.getStringByField(FieldConstant.LEVEL_FIELD);
		log.content = tuple.getStringByField(FieldConstant.CONTENT_FIELD);
		log.event = tuple.getStringByField(FieldConstant.EVENT_FIELD);
		log.uid = tuple.getStringByField(FieldConstant.UID_FIELD);
		log.gid = tuple.getStringByField(FieldConstant.GID_FIELD);
		log.company = tuple.getStringByField(FieldConstant.COMPANY_FIELD);
		log.agent = tuple.getStringByField(FieldConstant.AGENT_FIELD);
		log.result = tuple.getStringByField(FieldConstant.RESULT_FIELD);
		log.ctx = tuple.getStringByField(FieldConstant.CTX_FIELD);
		log.ip = tuple.getStringByField(FieldConstant.IP_FIELD);
		log.device = tuple.getStringByField(FieldConstant.DEVICE_FIELD);
		log.devid = tuple.getStringByField(FieldConstant.DEVICE_ID_FIELD);
		log.version = tuple.getStringByField(FieldConstant.VERSION_FIELD);
		log.imsi = tuple.getStringByField(FieldConstant.IMSI_FIELD);
		log.expect_payload = tuple.getStringByField(FieldConstant.EXPECT_PAYLOAD_FIELD);
		log.target = tuple.getStringByField(FieldConstant.TARGET_FIELD);
		log.target_got = tuple.getStringByField(FieldConstant.TARGET_GOT_FIELD);
		log.target_dent = tuple.getStringByField(FieldConstant.TARGET_DENT_FIELD);
		log.count = tuple.getStringByField(FieldConstant.COUNT_FIELD);
		log.sw = tuple.getStringByField(FieldConstant.SW_FIELD);
		log.value = tuple.getStringByField(FieldConstant.VALUE_FIELD);
		return log;
	}

	public String server;
	public String datetime;
	public Long	  timestamp;
	public String level;
	public String content;
	public String event;
	public String uid;
	public String gid;
	public String company;
	public String agent;
	public String result;
	public String ctx;
	public String ip;
	public String device;
	public String devid;
	public String version;
	public String imsi;
	public String expect_payload;
	public String target;
	public String target_got;
	public String target_dent;
	public String count;
	public String sw;
	public String value;

	public Values toValues() {
		return new Values(
			server,
			datetime,
			level,
			content,
			event,
			uid,
			gid,
			company,
			agent,
			result,
			ctx,
			ip,
			device,
			devid,
			version,
			imsi,
			expect_payload,
			target,
			target_got,
			target_dent,
			count,
			sw,
			value
				);
	}
}
