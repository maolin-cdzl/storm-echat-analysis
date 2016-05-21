package com.echat.storm.analysis;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Date;
import java.util.HashMap;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.PatternMatcherInput;


public class LogSpliterProcesser {
	public interface LogSpliterInterface {
		void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content);
	}

	private static final Logger log = LoggerFactory.getLogger(LogSpliterProcesser.class);
    private OutputCollector collector;
	private PatternCompiler compiler;
	private Pattern keyValuePattern;
	private HashMap<String,LogSpliterInterface> processers;

	public LogSpliterProcesser(OutputCollector collector) {
		this.collector = collector;
		compiler = new Perl5Compiler();

		try {
			keyValuePattern = compiler.compile("(\\w+)=\\(([^\\)]*)\\)");
		} catch( MalformedPatternException e ) {
			log.error(e.getMessage());
		}

		processers = new HashMap<String,LogSpliterInterface>();
		processers.put(AnalysisTopologyConstranst.EVENT_GET_MIC,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processGetMic(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_RELEASE_MIC,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processReleaseMic(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_DENT_MIC,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processDentMic(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_LOSTMIC_AUTO,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processLostMicAuto(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_LOSTMIC_REPLACE,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processLostMicReplace(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_RELOGIN,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processRelogin(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_LOGIN,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processLogin(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_LOGOUT,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processLogout(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_BROKEN,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processBroken(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_LOGIN_FAILED,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processLoginFailed(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_JOIN_GROUP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processJoinGroup(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_LEAVE_GROUP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processLeaveGroup(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CALL,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processCall(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_QUICKDIAL,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processQuickDial(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_QUERY_MEMBERS,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processQueryMembers(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_QUERY_GROUP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processQueryGroup(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_QUERY_USER,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processQueryUser(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_QUERY_DEPARTMENT,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processQueryDepartment(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_QUERY_ENTERPISE_GROUP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processQueryEnterpriseGroup(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CHANGE_NAME,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processChangeName(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CHANGE_PWD,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processChangePwd(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CHANGE_PWD_FAILED,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processChangePwdFailed(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CONTACT_REQ,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processContactReq(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CONTACT_REP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processContactRep(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CONTACT_RM,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processContactRM(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_DISPATCH,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processDispatch(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CONFIG,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processConfig(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_TAKE_MIC,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processTakeMic(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CREATE_GROUP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processCreateGroup(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_RM_GROUP,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processRmGroup(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_EMPOWER,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processEmpower(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_EMPOWER_FAILED,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processEmpowerFailed(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_DEPRIVE,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processDeprive(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_DEPRIVE_FAILED,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processDepriveFailed(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processChangeGroupName(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME_FAILED,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processChangeGroupNameFailed(input,app,date,content);
			}
		});
		processers.put(AnalysisTopologyConstranst.EVENT_WORKSHEET_POST,new LogSpliterInterface() {
			@Override
			public void process(LogSpliterProcesser processer,Tuple input,String app,Date date,String content) {
				processer.processPostWorksheet(input,app,date,content);
			}
		});
	}

	public void proc(String ev,Tuple input,String app,Date date,String content) {
		LogSpliterInterface p = processers.get(ev);
		if( p != null ) {
			p.process(this,input,app,date,content);
		}
	}

	public Map<String,String> getKeys(String content) {
		Map<String,String> keys = new HashMap<String,String>();
		PatternMatcher pm = new Perl5Matcher();
		PatternMatcherInput matcherInput = new PatternMatcherInput(content);  

		while( pm.contains(matcherInput,keyValuePattern) ) {
			MatchResult mr = pm.getMatch();  
			keys.put(mr.group(1),mr.group(2));
		}
		return keys;
	}
	public void processGetMic(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");
		String target = keys.get("target");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_GET_MIC,uid,gid,target));
		}
	}

	public void processDentMic(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");
		String target = keys.get("target");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_DENT_MIC,uid,gid,target));
		}
	}

	public void processReleaseMic(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");
		String target = keys.get("target");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_RELEASE_MIC,uid,gid,target));
		}
	}

	public void processLostMicAuto(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");
		String target = keys.get("target");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_LOSTMIC_AUTO,uid,gid,target));
		}
	}

	public void processLostMicReplace(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");
		String target = keys.get("target");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_SPEAK,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_LOSTMIC_REPLACE,uid,gid,target));
		}
	}

	public void processLogin(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String ctx = keys.get("context");
		String ip = keys.get("address");
		String version = keys.get("version");
		String device = keys.get("device");
		String meid = keys.get("meid");
		String esn = keys.get("esn");
		String imsi = keys.get("imsi");
		String expect_payload = keys.get("expect_payload");

		String devid = null;
		if( meid != null ) {
			devid = "meid-" + meid;
		} else if( esn != null ) {
			devid = "esn-" + esn;
		}
		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_ONLINE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_LOGIN,
						uid,ctx,ip,device,devid,version,imsi,expect_payload,null));
		}
	}

	public void processRelogin(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String ctx = keys.get("context");
		String ip = keys.get("address");
		String version = keys.get("version");
		String device = keys.get("device");
		String meid = keys.get("meid");
		String esn = keys.get("esn");
		String imsi = keys.get("imsi");
		String expect_payload = keys.get("expect_payload");

		String devid = null;
		if( meid != null ) {
			devid = "meid-" + meid;
		} else if( esn != null ) {
			devid = "esn-" + esn;
		}
		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_ONLINE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_RELOGIN,
						uid,ctx,ip,device,devid,version,imsi,expect_payload,null));
		}
	}

	public void processLogout(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String ip = keys.get("address");
		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_ONLINE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_LOGOUT,
						uid,null,ip,null,null,null,null,null,null));
		}
	}

	public void processBroken(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String ip = keys.get("address");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_ONLINE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_BROKEN,
						uid,null,ip,null,null,null,null,null,null));
		}
	}

	public void processLoginFailed(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		if( uid == null ) {
			uid = keys.get("account");
		}
		String ctx = keys.get("context");
		String ip = keys.get("address");
		String version = keys.get("version");
		String device = keys.get("device");
		String meid = keys.get("meid");
		String esn = keys.get("esn");
		String imsi = keys.get("imsi");
		String expect_payload = keys.get("expect_payload");
		String reason = keys.get("result");

		String devid = null;
		if( meid != null ) {
			devid = "meid-" + meid;
		} else if( esn != null ) {
			devid = "esn-" + esn;
		}
		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_LOGIN_FAILED,input,new Values(
						app,date,reason,
						uid,ctx,ip,device,devid,version,imsi,expect_payload));
		}
	}

	public void processJoinGroup(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_GROUP,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_JOIN_GROUP,uid,gid));
		}
	}

	public void processLeaveGroup(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");

		if( uid != null && gid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_GROUP,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_LEAVE_GROUP,uid,gid));
		}
	}

	public void processCall(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("targets");
		String called = keys.get("called");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_CALL,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CALL,
						uid,target,called));
		}
	}

	public void processQuickDial(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("targets");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_CALL,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUICKDIAL,
						uid,target,null));
		}
	}

	public void processQueryGroup(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String count = keys.get("gcount");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUERY_GROUP,
						uid,count,null));
		}
	}

	public void processQueryMembers(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gids");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUERY_MEMBERS,
						uid,null,target));
		}
	}

	public void processQueryContact(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String count = keys.get("count");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUERY_CONTACT,
						uid,count,null));
		}
	}
	public void processQueryUser(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String count = keys.get("count");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUERY_USER,
						uid,count,null));
		}
	}
	public void processQueryDepartment(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String count = keys.get("count");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUERY_DEPARTMENT,
						uid,count,null));
		}
	}

	public void processQueryEnterpriseGroup(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String count = keys.get("count");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_QUERY,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_QUERY_ENTERPISE_GROUP,
						uid,count,null));
		}
	}

	public void processChangeName(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target_got = keys.get("name");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CHANGE_NAME,
						uid,null,target_got,null));
		}
	}

	public void processChangePwd(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("oldpwd");
		String target_got = keys.get("newpwd");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CHANGE_PWD,
						uid,target,target_got,null));
		}
	}

	public void processChangePwdFailed(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("oldpwd");
		String target_dent = keys.get("newpwd");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CHANGE_PWD_FAILED,
						uid,target,null,target_dent));
		}
	}

	public void processContactReq(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("requested");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CONTACT_REQ,
						uid,target,target_got,null));
		}
	}

	public void processContactRep(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("accept");
		String target_got = keys.get("paired");
		String target_dent = keys.get("refuse");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CONTACT_REP,
						uid,target,target_got,target_dent));
		}
	}

	public void processContactRM(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_PROFILE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CONTACT_RM,
						uid,target,null,null));
		}
	}

	public void processDispatch(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String gid = keys.get("gid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_DISPATCH,
						uid,target,target_got,null,null,gid));
		}
	}

	public void processConfig(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");
		String audio = keys.get("audio");
		String location = keys.get("location");
		String period = keys.get("period");

		if( uid == null ) {
			return;
		}

		if( audio != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_SW_AUDIO,
						uid,target,target_got,null,audio,null));
		} else if( location != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_SW_GPS,
						uid,target,target_got,null,location,period));
		}
	}

	public void processTakeMic(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("dispatched");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_TAKE_MIC,
						uid,target,target_got,null,null,null));
		}
	}

	public void processCreateGroup(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String value = keys.get("name");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CREATE_GROUP,
						uid,target,null,null,null,value));
		}
	}

	public void processRmGroup(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String target_got = keys.get("deleted");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_RM_GROUP,
						uid,target,target_got,null,null,null));
		}
	}

	public void processEmpower(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("added");
		String value = keys.get("gid");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_EMPOWER,
						uid,target,target_got,null,null,value));
		}
	}

	public void processEmpowerFailed(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String value = keys.get("gid");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_EMPOWER_FAILED,
						uid,target,null,null,null,value));
		}
	}

	public void processDeprive(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String target_got = keys.get("remove");
		String value = keys.get("gid");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_DEPRIVE,
						uid,target,target_got,null,null,value));
		}
	}

	public void processDepriveFailed(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String value = keys.get("gid");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_DEPRIVE_FAILED,
						uid,target,null,null,null,value));
		}
	}

	public void processChangeGroupName(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String value = keys.get("name");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME,
						uid,target,null,null,null,value));
		}
	}

	public void processChangeGroupNameFailed(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("gid");
		String value = keys.get("name");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_MANAGE,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_CHANGE_GROUP_NAME_FAILED,
						uid,target,null,null,null,value));
		}
	}

	public void processPostWorksheet(Tuple input,String app,Date date,String content) {
		Map<String,String> keys = getKeys(content);
		String uid = keys.get("uid");
		String target = keys.get("target");
		String count = keys.get("msgcount");

		if( uid != null ) {
			collector.emit(AnalysisTopologyConstranst.STREAM_EVENT_GROUP_WORKSHEET,input,new Values(
						app,date,AnalysisTopologyConstranst.EVENT_WORKSHEET_POST,
						uid,target,count));
		}
	}

}

