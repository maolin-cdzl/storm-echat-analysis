package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.io.Serializable;

import com.google.gson.Gson;


public class ServerLoadList implements Serializable {
	public String							server;
	public LinkedList<ServerLoadBucket>		loads;
	public Date								timeLine;

	public ServerLoadList() {
		this.loads = new LinkedList<ServerLoadBucket>();
	}

	public ServerLoadList(final String server) {
		this.server = server;
		this.loads = new LinkedList<ServerLoadBucket>();
	}


	public void count(final String e,final Date time,final String ev) {
		if( server == null ) {
			server = e;
		} else if( ! server.equals(e) ) {
			throw new RuntimeException("Can not count event for different server!");
		}

		count(time,ev);
	}

	public void count(final Date time,final String ev) {
		for(ServerLoadBucket b : loads) {
			if( b.time.equals(time) ) {
				b.count(ev);
				return;
			}
		}
	
		ServerLoadBucket b = new ServerLoadBucket(server,time);
		b.count(ev);

		loads.add(getInsertPos(time),b);
	}

	public int size() {
		int s = 0;
		for(ServerLoadBucket b : loads) {
			s += b.size();
		}
		return s;
	}

	public ServerLoadList merge(ServerLoadBucket bucket) {
		if( !server.equals(bucket.server) ) {
			throw new RuntimeException("Can not merge different server!");
		}

		for(ServerLoadBucket e : loads) {
			if( e.time.equals(bucket.time) ) {
				e.merge(bucket);
				return this;
			}
		}

		loads.add(getInsertPos(bucket.time),bucket);
		return this;
	}

	public ServerLoadList merge(ServerLoadList list) {
		if( ! server.equals(list.server) ) {
			throw new RuntimeException("Can not merge different server list!");
		}
		for(ServerLoadBucket b : list.loads) {
			merge(b);
		}
		return this;
	}

	static public ServerLoadList merge(ServerLoadList l1,ServerLoadList l2) {
		if( l1.size() >= l2.size() ) {
			return l1.merge(l2);
		} else {
			return l2.merge(l1);
		}
	}

	public List<String[]> toReports(Gson gson,String dateFormat) {
		if( loads.size() == 0 ) {
			return null;
		}

		LinkedList<String[]> reports = new LinkedList<String[]>();

		for(ServerLoadBucket b : loads) {
			if( b.size() == 0 ) {
				continue;
			}
			String[] report = b.toReport(gson,dateFormat);
			if( report != null ) {
				reports.add(report);
			}
		}
		if( reports.size() > 0 ) {
			return reports;
		} else {
			return null;
		}
	}

	public ServerLoadBucket find(Date time) {
		for(ServerLoadBucket b : loads) {
			if( b.time.equals(time) ) {
				return b;
			}
		}
		return null;
	}

	public ServerLoadBucket getEarliest() {
		if( ! loads.isEmpty() ) {
			return loads.get(0);
		} else {
			return null;
		}
	}

	public ServerLoadBucket getLatest() {
		if( ! loads.isEmpty() ) {
			return loads.get(loads.size() - 1);
		} else {
			return null;
		}
	}

	public Date getEarliestDate() {
		if( ! loads.isEmpty() ) {
			return loads.get(0).time;
		} else {
			return null;
		}
	}

	public Date getLatestDate() {
		if( ! loads.isEmpty() ) {
			return loads.get(loads.size() - 1).time;
		} else {
			return null;
		}
	}

	public int getWindow() {
		if( loads.isEmpty() ) {
			return 0;
		}
		return (int)((loads.get(loads.size() -1).time.getTime() - loads.get(0).time.getTime()) / 1000);
	}

	public ServerLoadBucket popEarliest() {
		if( loads.isEmpty() ) {
			return null;
		}
		ServerLoadBucket b = loads.get(0);
		loads.remove(0);
		timeLine = b.time;
		return b;
	}

	public boolean isTooOld(Date time) {
		if( timeLine == null )
			return false;
		return timeLine.after(time);
	}

	private int getInsertPos(Date time) {
		int pos = 0;
		
		while(pos < loads.size()) {
			if( loads.get(pos).time.after(time) ) {
				break;
			}
			pos++;
		}
		return pos;
	}
}

