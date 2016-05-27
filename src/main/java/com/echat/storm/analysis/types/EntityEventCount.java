package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class EntityEventCount {

	public static class EntityEventTimeBucket {
		public String						entity;
		public Date							bucket;
		public HashMap<String,Long>			counter = new HashMap<String,Long>();

		public EntityEventTimeBucket() {
		}

		public void incCount(final String ev) {
			Long c = counter.get(ev);
			if( c == null ) {
				c = 1L;
			} else {
				c += 1L;
			}
			counter.put(ev,c);
		}

		public void merge(EntityEventTimeBucket other) {
			for(Map.Entry<String,Long> entry : other.counter.entrySet()) {
				Long c = counter.get(entry.getKey());
				if( c == null ) {
					c = entry.getValue();
				} else {
					c += entry.getValue();
				}
				counter.put(entry.getKey(),c);
			}
		}

		public String toJson(Gson gson) {
			if( counter.size() == 0 ) {
				return null;
			}
			return gson.toJson(this);
		}

		static public EntityEventTimeBucket fromJson(Gson gson,String str) {
			return gson.fromJson(str,EntityEventTimeBucket.class);
		}
	}

	private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
	private HashMap<String,ArrayList<EntityEventTimeBucket>>			statistics = new HashMap<String,ArrayList<EntityEventTimeBucket>>();

	public void stat(final String entity,final Date bucket,final String ev) {
		ArrayList<EntityEventTimeBucket> l = statistics.get(entity);
		if( l == null ) {
			l = new ArrayList<EntityEventTimeBucket>();
			statistics.put(entity,l);
		}

		for(EntityEventTimeBucket e : l) {
			if( e.bucket.equals(bucket) ) {
				e.incCount(ev);
				return;
			}
		}
	
		EntityEventTimeBucket e = new EntityEventTimeBucket();
		e.entity = entity;
		e.bucket = bucket;
		e.incCount(ev);

		l.add(getInsertPos(l,bucket),e);
	}

	public void merge(EntityEventTimeBucket other) {
		ArrayList<EntityEventTimeBucket> l = statistics.get(other.entity);
		if( l == null ) {
			l = new ArrayList<EntityEventTimeBucket>();
			l.add(other);
			statistics.put(other.entity,l);
			return;
		}

		for(EntityEventTimeBucket e : l) {
			if( e.bucket.equals(other.bucket) ) {
				e.merge(other);
				return;
			}
		}

		l.add(getInsertPos(l,other.bucket),other);
	}

	public List<String> toJson() {
		if( statistics.size() == 0 ) {
			return null;
		}

		List<String> vals = new LinkedList<String>();
		for(List<EntityEventTimeBucket> l : statistics.values()) {
			for(EntityEventTimeBucket b : l) {
				vals.add(b.toJson(gson));
			}
		}
		return vals;
	}

	public EntityEventTimeBucket fromJson(String str) {
		return EntityEventTimeBucket.fromJson(gson,str);
	}

	private int getInsertPos(List<EntityEventTimeBucket> l,Date bucket) {
		int pos = 0;
		
		while(pos < l.size()) {
			if( l.get(pos).bucket.after(bucket) ) {
				break;
			}
			pos++;
		}
		return pos;
	}
}

