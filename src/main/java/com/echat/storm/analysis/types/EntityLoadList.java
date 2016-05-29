package com.echat.storm.analysis.types;

import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.io.Serializable;

import com.google.gson.Gson;


public class EntityLoadList implements Serializable {
	public String							entity;
	public LinkedList<EntityLoadBucket>		loads;
	public Date								timeLine;

	public EntityLoadList() {
		this.loads = new LinkedList<EntityLoadBucket>();
	}

	public EntityLoadList(final String entity) {
		this.entity = entity;
		this.loads = new LinkedList<EntityLoadBucket>();
	}


	public void count(final String e,final Date time,final String ev) {
		if( entity == null ) {
			entity = e;
		} else if( ! entity.equals(e) ) {
			throw new RuntimeException("Can not count event for different entity!");
		}

		count(time,ev);
	}

	public void count(final Date time,final String ev) {
		for(EntityLoadBucket b : loads) {
			if( b.time.equals(time) ) {
				b.count(ev);
				return;
			}
		}
	
		EntityLoadBucket b = new EntityLoadBucket(entity,time);
		b.count(ev);

		loads.add(getInsertPos(time),b);
	}

	public int size() {
		int s = 0;
		for(EntityLoadBucket b : loads) {
			s += b.size();
		}
		return s;
	}

	public EntityLoadList merge(EntityLoadBucket bucket) {
		if( !entity.equals(bucket.entity) ) {
			throw new RuntimeException("Can not merge different entity!");
		}

		for(EntityLoadBucket e : loads) {
			if( e.time.equals(bucket.time) ) {
				e.merge(bucket);
				return this;
			}
		}

		loads.add(getInsertPos(bucket.time),bucket);
		return this;
	}

	public EntityLoadList merge(EntityLoadList list) {
		if( ! entity.equals(list.entity) ) {
			throw new RuntimeException("Can not merge different entity list!");
		}
		for(EntityLoadBucket b : list.loads) {
			merge(b);
		}
		return this;
	}

	static public EntityLoadList merge(EntityLoadList l1,EntityLoadList l2) {
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

		for(EntityLoadBucket b : loads) {
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

	public EntityLoadBucket find(Date time) {
		for(EntityLoadBucket b : loads) {
			if( b.time.equals(time) ) {
				return b;
			}
		}
		return null;
	}

	public EntityLoadBucket getEarliest() {
		if( ! loads.isEmpty() ) {
			return loads.get(0);
		} else {
			return null;
		}
	}

	public EntityLoadBucket getLatest() {
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

	public EntityLoadBucket popEarliest() {
		if( loads.isEmpty() ) {
			return null;
		}
		EntityLoadBucket b = loads.get(0);
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

