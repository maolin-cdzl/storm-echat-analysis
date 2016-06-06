package com.echat.storm.analysis.types;

import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokenEvent {
	private static final Logger log = LoggerFactory.getLogger(BrokenEvent.class);

	public String			server;
	public String			uid;
	public Date				date;
	public Long				offtime;
	public String			ctx;
	public String			ip;
	public String			device;
	public String			devid;
	public String			version;
	public String			imsi;

	static public BrokenEvent create(OnlineEvent login,long offtime) {
		BrokenEvent broken = new BrokenEvent();
		broken.server = login.server;
		broken.date = login.date;
		broken.offtime = offtime;
		broken.uid = login.uid;
		broken.ctx = login.ctx;
		broken.ip = login.ip;
		broken.device = login.device;
		broken.devid = login.devid;
		broken.version = login.version;
		broken.imsi = login.imsi;

		return broken;
	}
}

