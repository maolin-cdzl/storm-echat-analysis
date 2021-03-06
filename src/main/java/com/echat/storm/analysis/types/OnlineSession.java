package com.echat.storm.analysis.types;

import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlineSession {
	private static final Logger log = LoggerFactory.getLogger(OnlineSession.class);

	public String			server;
	public String			login;
	public String			logout;
	public String			uid;
	public String			ctx;
	public String			ip;
	public String			device;
	public String			devid;
	public String			version;
	public String			imsi;
	public String			expect_pt;

	static public OnlineSession create(OnlineEvent login,OnlineEvent logout) {
		if( ! login.server.equals(logout.server) || !login.uid.equals(logout.uid) || login.getTimeStamp() > logout.getTimeStamp() ) {
			log.error("Bad login/logout event pair!");
			return null;
		}

		OnlineSession session = new OnlineSession();
		session.server = login.server;
		session.login = login.datetime;
		session.logout = logout.datetime;
		session.uid = login.uid;
		session.ctx = login.ctx;
		session.ip = login.ip;
		session.device = login.device;
		session.devid = login.devid;
		session.version = login.version;
		session.imsi = login.imsi;
		session.expect_pt = login.expect_pt;

		return session;
	}
}

