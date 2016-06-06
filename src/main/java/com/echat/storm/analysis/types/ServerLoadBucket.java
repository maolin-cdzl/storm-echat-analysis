package com.echat.storm.analysis.types;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;

public class ServerLoadBucket implements Serializable {
	public String						server;
	public Date							time;
	public HashMap<String,Long>			events;

	public ServerLoadBucket() {
		events = new HashMap<String,Long>();
	}
	public ServerLoadBucket(final String e,final Date t) {
		server = e;
		time = t;
		events = new HashMap<String,Long>();
	}

	public void count(final String ev) {
		if( server == null ) {
			throw new RuntimeException("ServerLoadBucket Can not count before init");
		}
		Long c = events.get(ev);
		if( c == null ) {
			c = 1L;
		} else {
			c += 1L;
		}
		events.put(ev,c);
	}

	public int size() {
		return events.size();
	}

	public ServerLoadBucket merge(ServerLoadBucket other) {
		if( server == null ) {
			return other;
		}
		if( other.server == null ) {
			return this;
		}
		if( ! server.equals(other.server) ) {
			throw new RuntimeException("Can not merge to different server!");
		}

		for(Map.Entry<String,Long> entry : other.events.entrySet()) {
			Long c = events.get(entry.getKey());
			if( c == null ) {
				c = entry.getValue();
			} else {
				c += entry.getValue();
			}
			events.put(entry.getKey(),c);
		}
		return this;
	}

	public String[] toReport(Gson gson,String dateFormat) {
		if( events.size() == 0 ) {
			return null;
		}
		String[] report = new String[3];
		report[0] = server;
		report[1] = DateFormatUtils.format(time,dateFormat);
		report[2] = gson.toJson(this);
		return report;
	}

	static public ServerLoadBucket fromJson(Gson gson,String str) {
		return gson.fromJson(str,ServerLoadBucket.class);
	}

	static public ServerLoadBucket merge(ServerLoadBucket b1,ServerLoadBucket b2) {
		if( b1.size() >= b2.size() ) {
			return b1.merge(b2);
		} else {
			return b2.merge(b1);
		}
	}

}
