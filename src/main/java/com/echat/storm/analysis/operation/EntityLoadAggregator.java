package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;

import java.util.Date;
import java.util.Calendar;
import java.util.List;
import java.util.Arrays;
import java.text.ParseException;
import org.apache.commons.lang.time.DateFormatUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;


public class EntityLoadAggregator implements CombinerAggregator<EntityLoadBucket> {
	private static final Logger log = LoggerFactory.getLogger(EntityLoadAggregator.class);

	private Gson _gson;

	@Override
	public EntityLoadBucket init(TridentTuple tuple) {
		return EntityLoadBucket.fromJson(getGson(),tuple.getString(0));
	}

	@Override
	public EntityLoadBucket combine(EntityLoadBucket v1,EntityLoadBucket v2) {
		return EntityLoadBucket.merge(v1,v2);
	}

	@Override
	public EntityLoadBucket zero() {
		return new EntityLoadBucket();
	}

	private Gson getGson() {
		if( _gson == null ) {
			_gson = new GsonBuilder().setDateFormat(TopologyConstant.STD_DATETIME_FORMAT).create();
		}
		return _gson;
	}
}


