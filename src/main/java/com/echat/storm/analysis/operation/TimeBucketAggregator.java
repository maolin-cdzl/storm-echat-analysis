package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;

import java.util.Date;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Arrays;
import java.text.ParseException;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TimeBucketCounter {
	private static final Logger log = LoggerFactory.getLogger(TimeBucketCounter.class);

	public final String	entity;
	public Date			bucket = null;
	public HashMap<String,Long> counter = new HashMap<String,Long>();

	public TimeBucketCounter(final String entity,Date date,final String ev) {
		this.entity = entity;
		this.bucket = DateUtils.truncate(date,Calendar.SECOND);

		incCount(ev);
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

	public void reset(Date date,final String ev) {
		bucket = DateUtils.truncate(date,Calendar.SECOND);
		counter.clear();

		incCount(ev);
	}

	public void report(TridentCollector collector) {
		if( counter.size() > 0 ) {
			final String strBucket = DateFormatUtils.format(bucket,"yyyy-MM-dd HH:mm:ss");
			Gson gson = new Gson();
			final String counts = gson.toJson(counter);

			log.info("emit " + entity + " for " + strBucket);
			collector.emit(new Values(entity,strBucket,counts));
		}
	}
}

public class TimeBucketAggregator extends BaseAggregator<HashMap<String,TimeBucketCounter>> {
	private static final Logger log = LoggerFactory.getLogger(TimeBucketAggregator.class);
	private static final String[] DATETIME_FORMAT = new String[] { 
		"yyyy/MM/dd HH:mm:ss",
		"yyyy-MM-dd HH:mm:ss"
   	};

	private String _entityField;
	private String _dateField;
	private String _evField;

	public TimeBucketAggregator(final String entityField,final String dateField,final String evField) {
		_entityField = entityField;
		_dateField = dateField;
		_evField = evField;
	}

	@Override
	public HashMap<String,TimeBucketCounter> init(Object batchId, TridentCollector collector) {
		return new HashMap<String,TimeBucketCounter>();
	}

	@Override
    public void aggregate(HashMap<String,TimeBucketCounter> states, TridentTuple tuple, TridentCollector collector) {
		if( !tuple.contains(_entityField) || !tuple.contains(_dateField) || !tuple.contains(_evField) ) {
			log.warn("Can not found all need fields in: " + Arrays.toString(tuple.getFields().toList().toArray()));
			return;
		}

		final String entity = tuple.getStringByField(_entityField);
		final String datetime = tuple.getStringByField(_dateField);
		final String ev = tuple.getStringByField(_evField);

		if( entity == null || datetime == null || ev == null ) {
			return;
		}

		Date date;
		try {
			date = DateUtils.parseDate(datetime,DATETIME_FORMAT);
		} catch( ParseException e ) {
			log.warn("Bad datetime format: " + datetime);
			return;
		}

		TimeBucketCounter state = states.get(entity);
		
		if( state == null ) {
			log.info("Create state for: " + entity + "." + ev);
			state = new TimeBucketCounter(entity,date,ev);
			states.put(entity,state);
		} else {
			Date bucket = DateUtils.truncate(date,Calendar.SECOND);
			if( state.bucket.equals(bucket) ) {
				state.incCount(ev);
			} else {
				log.info(entity + " bucket jump from " + state.bucket + " to " + bucket);
				state.report(collector);
				state.reset(date,ev);
			}
		}

	}

	@Override
    public void complete(HashMap<String,TimeBucketCounter> states, TridentCollector collector) {
		for(TimeBucketCounter state : states.values()) {
			state.report(collector);
		}
	}
}

