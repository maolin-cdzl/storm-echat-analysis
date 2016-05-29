package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityLoadMap implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(EntityLoadMap.class);

	private HashMap<String,EntityLoadList>			loadmap = new HashMap<String,EntityLoadList>();

	public void count(final String entity,final Date time,final String ev) {
		EntityLoadList l = loadmap.get(entity);
		if( l == null ) {
			l = new EntityLoadList(entity);
			loadmap.put(entity,l);
		}

		l.count(time,ev);
	}

	public int size() {
		int s = 0;
		for(EntityLoadList l : loadmap.values()) {
			s += l.size();
		}
		return s;
	}

	public EntityLoadMap merge(EntityLoadBucket bucket) {
		EntityLoadList l = loadmap.get(bucket.entity);
		if( l == null ) {
			l = new EntityLoadList(bucket.entity);
			loadmap.put(bucket.entity,l);
		}

		l.merge(bucket);
		return this;
	}

	public EntityLoadMap merge(EntityLoadList list) {
		EntityLoadList l = loadmap.get(list.entity);
		if( l == null ) {
			l = new EntityLoadList(list.entity);
			loadmap.put(l.entity,l);
		}

		l.merge(list);
		return this;
	}

	public EntityLoadMap merge(EntityLoadMap map) {
		for(EntityLoadList l : map.loadmap.values()) {
			merge(l);
		}
		return this;
	}

	static public EntityLoadMap merge(EntityLoadMap v1,EntityLoadMap v2) {
		if( v1.size() >= v2.size() ) {
			return v1.merge(v2);
		} else {
			return v2.merge(v1);
		}
	}

	public List<String[]> toReports(Gson gson,String dateFormat) {
		if( loadmap.size() == 0 ) {
			return null;
		}

		LinkedList<String[]> reports = new LinkedList<String[]>();
		for(EntityLoadList list : loadmap.values()) {
			List<String[]> r = list.toReports(gson,dateFormat);
			if( r != null ) {
				reports.addAll(r);
			}
		}

		if( reports.size() > 0 ) {
			return reports;
		} else {
			return null;
		}
	}

	public EntityLoadBucket find(String entity,Date time) {
		EntityLoadList l = loadmap.get(entity);
		if( l != null ) {
			return l.find(time);
		} else {
			return null;
		}
	}

	public EntityLoadList find(String entity) {
		return loadmap.get(entity);
	}
}

