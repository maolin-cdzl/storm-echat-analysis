package com.echat.storm.analysis.types;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;

public class EntityLoadBucket implements Serializable {
	public String						entity;
	public Date							time;
	public HashMap<String,Long>			events;

	public EntityLoadBucket() {
		events = new HashMap<String,Long>();
	}
	public EntityLoadBucket(final String e,final Date t) {
		entity = e;
		time = t;
		events = new HashMap<String,Long>();
	}

	public void count(final String ev) {
		if( entity == null ) {
			throw new RuntimeException("EntityLoadBucket Can not count before init");
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

	public EntityLoadBucket merge(EntityLoadBucket other) {
		if( entity == null ) {
			return other;
		}
		if( other.entity == null ) {
			return this;
		}
		if( ! entity.equals(other.entity) ) {
			throw new RuntimeException("Can not merge to different entity!");
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
		report[0] = entity;
		report[1] = DateFormatUtils.format(time,dateFormat);
		report[2] = gson.toJson(this);
		return report;
	}

	static public EntityLoadBucket fromJson(Gson gson,String str) {
		return gson.fromJson(str,EntityLoadBucket.class);
	}

	static public EntityLoadBucket merge(EntityLoadBucket b1,EntityLoadBucket b2) {
		if( b1.size() >= b2.size() ) {
			return b1.merge(b2);
		} else {
			return b2.merge(b1);
		}
	}

}
