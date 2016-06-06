package com.echat.storm.analysis.spout;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import com.echat.storm.analysis.constant.FieldConstant;
import com.echat.storm.analysis.constant.EventConstant;
import com.echat.storm.analysis.constant.TopologyConstant;
import com.echat.storm.analysis.types.PttSvcLog;
import com.echat.storm.analysis.utils.GetKeyValues;
import com.echat.storm.analysis.utils.UserOrgInfoReader;
import com.echat.storm.analysis.utils.UserOrgInfoReader.OrganizationInfo;


public class PttsvcLogInfoScheme implements Scheme {
	private static final Logger logger = LoggerFactory.getLogger(PttsvcLogInfoScheme.class);

	private interface EventParser extends Serializable {
		boolean parse(PttSvcLog log,Map<String,String> keys);
	}

	private static final String BASE_PATTERN = "^(\\w+)\\s+(\\d{4}[/:\\.\\-\\\\]\\d{2}[/:\\.\\-\\\\]\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\.(\\d+)\\s(\\w+)[^\"]*\"([^\"]+)\"";

	//take care of the order
	private static final String[][] LOG_PREFIX_TO_EVENT = {
		{"GET MIC",EventConstant.EVENT_GET_MIC},
		{"RELEASE MIC",EventConstant.EVENT_RELEASE_MIC},
		{"DENT MIC",EventConstant.EVENT_DENT_MIC},
		{"LOSTMIC REPLACE",EventConstant.EVENT_LOSTMIC_REPLACE},
		{"LOSTMIC AUTO",EventConstant.EVENT_LOSTMIC_AUTO},
		{"RELOGIN",EventConstant.EVENT_RELOGIN},
		{"LOGIN FAILED",EventConstant.EVENT_LOGIN_FAILED},
		{"LOGIN",EventConstant.EVENT_LOGIN},
		{"LOGOUT BROKEN",EventConstant.EVENT_BROKEN},
		{"LOGOUT",EventConstant.EVENT_LOGOUT},
		{"QUERY MEMBERS",EventConstant.EVENT_QUERY_MEMBERS},
		{"QUERY GROUP",EventConstant.EVENT_QUERY_GROUP},
		{"QUERY CONTACTS",EventConstant.EVENT_QUERY_CONTACT},
		{"QUERY DEPARTMENT",EventConstant.EVENT_QUERY_DEPARTMENT},
		{"QUERY ENTERPRISE GROUP",EventConstant.EVENT_QUERY_ENTERPISE_GROUP},
		{"QUERY USER",EventConstant.EVENT_QUERY_USER},
		{"JOIN GROUP",EventConstant.EVENT_JOIN_GROUP},
		{"LEAVE GROUP",EventConstant.EVENT_LEAVE_GROUP},
		{"CALL",EventConstant.EVENT_CALL},
		{"QUICKDIAL",EventConstant.EVENT_QUICKDIAL},
		{"CHANGE NAME",EventConstant.EVENT_CHANGE_NAME},
		{"CHANGE PWD FAULT",EventConstant.EVENT_CHANGE_PWD_FAILED},
		{"CHANGE PWD",EventConstant.EVENT_CHANGE_PWD},
		{"CONTACT MAKE",EventConstant.EVENT_CONTACT_REQ},
		{"CONTACT RESPONSE",EventConstant.EVENT_CONTACT_REP},
		{"CONTACT REMOVE",EventConstant.EVENT_CONTACT_RM},
		{"DISPATCH",EventConstant.EVENT_DISPATCH},
		{"CONFIG",EventConstant.EVENT_CONFIG},
		{"TAKE MIC",EventConstant.EVENT_TAKE_MIC},
		{"CREATE GROUP",EventConstant.EVENT_CREATE_GROUP},
		{"DELETE GROUP",EventConstant.EVENT_RM_GROUP},
		{"EMPOWER FAULT",EventConstant.EVENT_EMPOWER_FAILED},
		{"EMPOWER",EventConstant.EVENT_EMPOWER},
		{"DEPRIVE FAULT",EventConstant.EVENT_DEPRIVE_FAILED},
		{"DEPRIVE",EventConstant.EVENT_DEPRIVE},
		{"MODIFY GROUP FAULT",EventConstant.EVENT_CHANGE_GROUP_NAME_FAILED},
		{"MODIFY GROUP",EventConstant.EVENT_CHANGE_GROUP_NAME},
		{"POST WORKSHEET",EventConstant.EVENT_WORKSHEET_POST}
	};

	static private HashMap<String,EventParser> EVENT_PARSER_MAP = createEventParserMap();

	private Pattern basePattern = null;
	private PatternMatcher pm = null;
	private GetKeyValues reg = null;
	private UserOrgInfoReader orgReader = null;

	public PttsvcLogInfoScheme() {
		PatternCompiler compiler = new Perl5Compiler();
		try {
			this.basePattern = compiler.compile(BASE_PATTERN);
		} catch( MalformedPatternException e ) {
		}
		pm = new Perl5Matcher();
		reg = new GetKeyValues();
		orgReader = new UserOrgInfoReader(TopologyConstant.REDIS_CONFIG);
	}

	@Override
	public Fields getOutputFields() {
		return PttSvcLog.newFields();
	}

	@Override
	public List<Object> deserialize(byte[] bytes) {
		String line = null;
		try {
			line = new String(bytes,"UTF-8");
		} catch( UnsupportedEncodingException e) {
			logger.warn("Unsupport tuple recved");
			return null;
		}

		PttSvcLog log = new PttSvcLog();
		if( ! baseParse(log,line) ) {
			return null;
		}
		if( log.event != null ) {
			EventParser p = EVENT_PARSER_MAP.get(log.event);
			if( p != null ) {
				Map<String,String> keys = reg.getKeys(log.content);
				if( ! p.parse(log,keys) ) {
					return null;
				}
				if( log.uid != null ) {
					OrganizationInfo org = orgReader.search(log.uid);
					if( org != null ) {
						log.company = org.company;
						log.agent = org.agent;
					}
				}
			}
		}
		return log.toValues();
	}

	private boolean baseParse(PttSvcLog log,String line) {
		if( pm.contains(line,basePattern) ) {
			MatchResult mr = pm.getMatch();
			log.server = mr.group(1);
			log.datetime = mr.group(2);
			try {
				log.timestamp = Long.parseLong(mr.group(3)) / 100L;
			} catch( NumberFormatException e ) {
				
				return false;
			}
			log.level = mr.group(4);
			log.content = mr.group(5);

			//logger.debug("Kafka tuple: " + entity + "\t" + datetime + "\t" + level + "\t" + content);
			
			if( log.level.equals("INFO") ) {
				for(int i=0; i < LOG_PREFIX_TO_EVENT.length; i++) {
					if( log.content.startsWith(LOG_PREFIX_TO_EVENT[i][0]) ) {
						// split CONFIG event to audio/location sw event
						if( LOG_PREFIX_TO_EVENT[i][1].equals(EventConstant.EVENT_CONFIG) ) {
							if( log.content.contains("audio") ) {
								log.event = EventConstant.EVENT_SW_AUDIO;
								return true;
							} else if( log.content.contains("location") ) {
								log.event = EventConstant.EVENT_SW_GPS;
								return true;
							}
						} else {
							log.event = LOG_PREFIX_TO_EVENT[i][1];
							return true;
						}
					}
				}
			} else {
				return true;
			}
		}
		return false;
	}

	static private HashMap<String,EventParser> createEventParserMap() {
		HashMap<String,EventParser> map = new HashMap<String,EventParser>();

		EventParser loginParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				if( log.uid == null ) {
					return false;
				}
				log.ctx = keys.get("context");
				log.ip = keys.get("address");
				log.version = keys.get("version");
				log.device = keys.get("device");
				log.expect_payload = keys.get("expect_payload");
				
				if( keys.containsKey("meid") ) {
					log.devid = "meid-" + keys.get("meid");
				} else if( keys.containsKey("esn") ) {
					log.devid = "esn-" + keys.get("esn");
				}
				log.imsi = keys.get("imsi");
				log.result = keys.get("result");

				return true;
			}
		};
		map.put(EventConstant.EVENT_LOGIN,loginParser);
		map.put(EventConstant.EVENT_RELOGIN,loginParser);
		map.put(EventConstant.EVENT_LOGIN_FAILED,loginParser);

		EventParser logoutParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				log.ip = keys.get("address");
				return ( log.uid != null );
			}
		};
		map.put(EventConstant.EVENT_BROKEN,logoutParser);
		map.put(EventConstant.EVENT_LOGOUT,logoutParser);

		EventParser speakParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				log.gid = keys.get("gid");
				log.target = keys.get("target");

				return ( log.uid != null && log.gid != null );
			}
		};
		map.put(EventConstant.EVENT_GET_MIC,speakParser);
		map.put(EventConstant.EVENT_DENT_MIC,speakParser);
		map.put(EventConstant.EVENT_RELEASE_MIC,speakParser);
		map.put(EventConstant.EVENT_LOSTMIC_AUTO,speakParser);
		map.put(EventConstant.EVENT_LOSTMIC_REPLACE,speakParser);
		
		EventParser groupParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				log.gid = keys.get("gid");
				return ( log.uid != null && log.gid != null );
			}
		};
		map.put(EventConstant.EVENT_JOIN_GROUP,groupParser);
		map.put(EventConstant.EVENT_LEAVE_GROUP,groupParser);

		EventParser callParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				log.target = keys.get("targets");
				log.target_got = keys.get("called");
				return log.uid != null;
			}
		};
		map.put(EventConstant.EVENT_CALL,callParser);
		map.put(EventConstant.EVENT_QUICKDIAL,callParser);


		EventParser queryParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				log.count = keys.get("count");
				log.target = keys.get("gids");
				if( log.count == null ) {
					log.count = keys.get("gcount");
				}

				return log.uid != null;
			}
		};
		map.put(EventConstant.EVENT_QUERY_USER,queryParser);
		map.put(EventConstant.EVENT_QUERY_GROUP,queryParser);
		map.put(EventConstant.EVENT_QUERY_CONTACT,queryParser);
		map.put(EventConstant.EVENT_QUERY_MEMBERS,queryParser);
		map.put(EventConstant.EVENT_QUERY_DEPARTMENT,queryParser);
		map.put(EventConstant.EVENT_QUERY_ENTERPISE_GROUP,queryParser);

		EventParser profileParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				if( EventConstant.EVENT_CHANGE_NAME.equals(log.event) ) {
					log.target_got = keys.get("name");
				} else if( EventConstant.EVENT_CHANGE_PWD.equals(log.event) ) {
					log.target = keys.get("oldpwd");
					log.target_got = keys.get("newpwd");
				} else if( EventConstant.EVENT_CHANGE_PWD_FAILED.equals(log.event) ) {
					log.target = keys.get("oldpwd");
					log.target_dent = keys.get("newpwd");
				} else if( EventConstant.EVENT_CONTACT_REQ.equals(log.event) ) {
					log.target = keys.get("target");
					log.target_got = keys.get("requested");
				} else if( EventConstant.EVENT_CONTACT_REP.equals(log.event) ) {
					log.target = keys.get("accept");
					log.target_got = keys.get("paired");
					log.target_dent = keys.get("refuse");
				} else if( EventConstant.EVENT_CONTACT_RM.equals(log.event) ) {
					log.target = keys.get("target");
				} else {
					return false;
				}
				return log.uid != null;
			}
		};
		map.put(EventConstant.EVENT_CHANGE_NAME,profileParser);
		map.put(EventConstant.EVENT_CHANGE_PWD,profileParser);
		map.put(EventConstant.EVENT_CHANGE_PWD_FAILED,profileParser);
		map.put(EventConstant.EVENT_CONTACT_REQ,profileParser);
		map.put(EventConstant.EVENT_CONTACT_REP,profileParser);
		map.put(EventConstant.EVENT_CONTACT_RM,profileParser);

		EventParser manageParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");

				if( EventConstant.EVENT_DISPATCH.equals(log.event) ) {
					log.value = keys.get("gid");
					log.target = keys.get("target");
					log.target_got = keys.get("dispatched");
				} else if( EventConstant.EVENT_SW_GPS.equals(log.event) ) {
					log.target = keys.get("target");
					log.target_got = keys.get("dispatched");
					log.sw = keys.get("location");
					log.value = keys.get("period");
				} else if( EventConstant.EVENT_SW_AUDIO.equals(log.event) ) {
					log.target = keys.get("target");
					log.target_got = keys.get("dispatched");
					log.sw = keys.get("audio");
				} else if( EventConstant.EVENT_TAKE_MIC.equals(log.event) ) {
					log.uid = keys.get("uid");
					log.target = keys.get("target");
					log.target_got = keys.get("dispatched");
				} else if( EventConstant.EVENT_CREATE_GROUP.equals(log.event) ) {
					log.target = keys.get("gid");
					log.value = keys.get("name");
				} else if( EventConstant.EVENT_RM_GROUP.equals(log.event) ) {
					log.target = keys.get("gid");
					log.target_got = keys.get("deleted");
				} else if( EventConstant.EVENT_EMPOWER.equals(log.event) ) {
					log.target = keys.get("target");
					log.target_got = keys.get("added");
					log.value = keys.get("gid");
				} else if( EventConstant.EVENT_EMPOWER_FAILED.equals(log.event) ) {
					log.target = keys.get("target");
					log.value = keys.get("gid");
				} else if( EventConstant.EVENT_DEPRIVE.equals(log.event) ) {
					log.target = keys.get("target");
					log.target_got = keys.get("remove");
					log.value = keys.get("gid");
				} else if( EventConstant.EVENT_DEPRIVE_FAILED.equals(log.event) ) {
					log.target = keys.get("target");
					log.value = keys.get("gid");
				} else if( EventConstant.EVENT_CHANGE_GROUP_NAME.equals(log.event) ) {
					log.target = keys.get("gid");
					log.value = keys.get("name");
				} else if( EventConstant.EVENT_CHANGE_GROUP_NAME_FAILED.equals(log.event) ) {
					log.target = keys.get("gid");
					log.value = keys.get("name");
				} else {
					return false;
				}
				return log.uid != null;
			}
		};

		map.put(EventConstant.EVENT_DISPATCH,manageParser);
		map.put(EventConstant.EVENT_SW_GPS,manageParser);
		map.put(EventConstant.EVENT_SW_AUDIO,manageParser);
		map.put(EventConstant.EVENT_TAKE_MIC,manageParser);
		map.put(EventConstant.EVENT_CREATE_GROUP,manageParser);
		map.put(EventConstant.EVENT_RM_GROUP,manageParser);
		map.put(EventConstant.EVENT_EMPOWER,manageParser);
		map.put(EventConstant.EVENT_EMPOWER_FAILED,manageParser);
		map.put(EventConstant.EVENT_DEPRIVE,manageParser);
		map.put(EventConstant.EVENT_DEPRIVE_FAILED,manageParser);
		map.put(EventConstant.EVENT_CHANGE_GROUP_NAME,manageParser);
		map.put(EventConstant.EVENT_CHANGE_GROUP_NAME_FAILED,manageParser);
		
		EventParser worksheetParser = new EventParser() {
			@Override
			public boolean parse(PttSvcLog log,Map<String,String> keys) {
				log.uid = keys.get("uid");
				log.target = keys.get("target");
				log.count = keys.get("msgcount");
				return log.uid != null;
			}
		};
		map.put(EventConstant.EVENT_WORKSHEET_POST,worksheetParser);

		return map;
	}
}
