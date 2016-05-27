package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import com.echat.storm.analysis.FieldsConstrants;

public class EventFilter extends BaseFilter {
	private static final Logger log = LoggerFactory.getLogger(EventFilter.class);
	private final String[] events;
//	private boolean first = false;

	public EventFilter(String event) {
		this.events = new String[]{ event };
	}

	public EventFilter(final String[] events) {
		this.events = events;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		if( events == null || events.length == 0 ) {
			log.error("Event list is null or empty");
			return false;
		}
		final String curEv = tuple.getStringByField(FieldsConstrants.EVENT_FIELD);

//		if( first ) {
//			first = false;
//			log.info("Event: " + curEv + " accepted: " + Arrays.toString(events));
//		}
		for(String ev : events) {
			if( ev.equals(curEv) ) {
				return true;
			}
		}
		return false;
	}
}


