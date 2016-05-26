package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import com.echat.storm.analysis.FieldsConstrants;

public class EventFilter extends BaseFilter {
	private final String[] events;

	public EventFilter(String event) {
		this.events = new String[]{ event };
	}

	public EventFilter(final String[] events) {
		this.events = events;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		final String curEv = tuple.getStringByField(FieldsConstrants.EVENT_FIELD);
		for(String ev : events) {
			if( ev.equals(curEv) ) {
				return true;
			}
		}
		return false;
	}
}


