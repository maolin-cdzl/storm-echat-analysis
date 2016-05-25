package com.echat.storm.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Date;
import java.text.ParseException;
import java.util.HashMap;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;


public class PttsvcLogSpliter extends BaseRichBolt {
	private static final Logger log = LoggerFactory.getLogger(PttsvcLogSpliter.class);
	private static final String[] DATETIME_FORMAT = new String[] { 
		"yyyy/MM/dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss"
   	};
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


    private OutputCollector collector;
	private HashMap<String,AppTpsCounter> counter;
	private LogSpliterProcesser processer;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this.collector = collector;
		processer = new LogSpliterProcesser(collector);
		counter = new HashMap<String,AppTpsCounter>();
	}

	@Override
	public void execute(Tuple input) {
		String app,datetime,level,content;

		try {
			app = input.getStringByField(FieldsConstrants.APP_FIELD);
			datetime = input.getStringByField(FieldsConstrants.DATETIME_FIELD);
			level = input.getStringByField(FieldsConstrants.LEVEL_FIELD);
			content = input.getStringByField(FieldsConstrants.CONTENT_FIELD);

			log.debug("PttsvcLogSpliter : " + app + "\t" + datetime + "\t" + level + "\t" + content);

		} catch( IllegalArgumentException e ) {
			log.warn(e.getMessage());
			collector.fail(input);
			return;
		}

		Date date;
		try {
			date = DateUtils.parseDate(datetime,DATETIME_FORMAT);
		} catch( ParseException e ) {
			log.warn("Bad datetime format: " + datetime);
			collector.fail(input);
			return;
		}

		AppTpsCounter c = getCounter(app,date);
		boolean report = false;

		if( level.equals("INFO") ) {
			report = parseEvent(input,c,app,date,content);
		} else if( level.equals("WARNING") ) {
			report = c.warn(date);
			collector.emit(AnalysisTopologyConstranst.STREAM_WARN,input,
					new Values(app,datetime,content));
		} else if( level.equals("ERROR") ) {
			report = c.error(date);
			collector.emit(AnalysisTopologyConstranst.STREAM_ERROR,input,
					new Values(app,datetime,content));
		} else if( level.equals("FATAL") ) {
			report = c.fatal(date);
			collector.emit(AnalysisTopologyConstranst.STREAM_FATAL,input,
					new Values(app,datetime,content));
		} else {
			collector.fail(input);
			return;
		}

		if( report ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_APP_LOAD,input,new Values(
				app,
				datetime,
				c.lastReport));
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(AnalysisTopologyConstranst.STREAM_APP_LOAD,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.APP_LOAD_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_WARN,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.CONTENT_FIELD
					));         
		declarer.declareStream(AnalysisTopologyConstranst.STREAM_ERROR,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.CONTENT_FIELD
					));         
		declarer.declareStream(AnalysisTopologyConstranst.STREAM_FATAL,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.CONTENT_FIELD
					));         

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.GID_FIELD,
					FieldsConstrants.TARGET_FIELD
					));         

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_ONLINE,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.CTX_FIELD,
					FieldsConstrants.IP_FIELD,
					FieldsConstrants.DEVICE_FIELD,
					FieldsConstrants.DEVICE_ID_FIELD,
					FieldsConstrants.VERSION_FIELD,
					FieldsConstrants.IMSI_FIELD,
					FieldsConstrants.EXPECT_PAYLOAD_FIELD,
					FieldsConstrants.REASON_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_LOGIN_FAILED,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.REASON_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.CTX_FIELD,
					FieldsConstrants.IP_FIELD,
					FieldsConstrants.DEVICE_FIELD,
					FieldsConstrants.DEVICE_ID_FIELD,
					FieldsConstrants.VERSION_FIELD,
					FieldsConstrants.IMSI_FIELD,
					FieldsConstrants.EXPECT_PAYLOAD_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_GROUP,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.GID_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_CALL,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.TARGET_GOT_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.COUNT_FIELD,
					FieldsConstrants.TARGET_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.TARGET_GOT_FIELD,
					FieldsConstrants.TARGET_DENT_FIELD
					));

		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.TARGET_GOT_FIELD,
					FieldsConstrants.TARGET_DENT_FIELD,
					FieldsConstrants.SW_FIELD,
					FieldsConstrants.VALUE_FIELD
					));
		declarer.declareStream(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_WORKSHEET,new Fields(
					FieldsConstrants.APP_FIELD,
					FieldsConstrants.DATETIME_FIELD,
					FieldsConstrants.EVENT_FIELD,
					FieldsConstrants.UID_FIELD,
					FieldsConstrants.TARGET_FIELD,
					FieldsConstrants.COUNT_FIELD
					));
	}

	private AppTpsCounter getCounter(final String app,final Date date) {
		AppTpsCounter c = counter.get(app);
		if( c == null ) {
			c = new AppTpsCounter(app,date);
			counter.put(app,c);
		}
		return c;
	}

	private boolean parseEvent(Tuple input,AppTpsCounter c,String app,Date date,String content) {
		boolean report = false;
		
		final String ev = getEvent(content);
		if( ev != null ) {
			processer.proc(ev,input,app,date,content);	
			c.event(date,ev);
		} else {
			if( AnalysisTopologyConstranst.DEBUG ) {
				log.info("No event: " + content);
			}
		}
		report = c.info(date);
		return report;
	}

	private String getEvent(String content) {
		for(int i=0; i < LOG_PREFIX_TO_EVENT.length; i++) {
			if( content.startsWith(LOG_PREFIX_TO_EVENT[i][0]) ) {
				return LOG_PREFIX_TO_EVENT[i][1];
			}
		}
		return null;
	}

}

