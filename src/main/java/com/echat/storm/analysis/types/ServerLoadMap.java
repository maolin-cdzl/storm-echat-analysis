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

public class ServerLoadMap implements Serializable {
	private static final Logger log = LoggerFactory.getLogger(ServerLoadMap.class);

	private HashMap<String,ServerLoadList>			loadmap = new HashMap<String,ServerLoadList>();

	public void count(final String server,final Date time,final String ev) {
		ServerLoadList l = loadmap.get(server);
		if( l == null ) {
			l = new ServerLoadList(server);
			loadmap.put(server,l);
		}

		l.count(time,ev);
	}

	public int size() {
		int s = 0;
		for(ServerLoadList l : loadmap.values()) {
			s += l.size();
		}
		return s;
	}

	public ServerLoadMap merge(ServerLoadBucket bucket) {
		ServerLoadList l = loadmap.get(bucket.server);
		if( l == null ) {
			l = new ServerLoadList(bucket.server);
			loadmap.put(bucket.server,l);
		}

		l.merge(bucket);
		return this;
	}

	public ServerLoadMap merge(ServerLoadList list) {
		ServerLoadList l = loadmap.get(list.server);
		if( l == null ) {
			l = new ServerLoadList(list.server);
			loadmap.put(l.server,l);
		}

		l.merge(list);
		return this;
	}

	public ServerLoadMap merge(ServerLoadMap map) {
		for(ServerLoadList l : map.loadmap.values()) {
			merge(l);
		}
		return this;
	}

	static public ServerLoadMap merge(ServerLoadMap v1,ServerLoadMap v2) {
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
		for(ServerLoadList list : loadmap.values()) {
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

	public ServerLoadBucket find(String server,Date time) {
		ServerLoadList l = loadmap.get(server);
		if( l != null ) {
			return l.find(time);
		} else {
			return null;
		}
	}

	public ServerLoadList find(String server) {
		return loadmap.get(server);
	}
}

