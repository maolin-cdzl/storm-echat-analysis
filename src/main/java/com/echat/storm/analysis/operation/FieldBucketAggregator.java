package com.echat.storm.analysis.operation;

import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;

import java.util.Date;
import java.util.Calendar;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Map;
import java.text.ParseException;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.*;


public class FieldBucketAggregator extends BaseAggregator<ServerLoadMap> {
	private static final Logger log = LoggerFactory.getLogger(FieldBucketAggregator.class);

	private final String _serverField;
	private final String _dateField;
	private final String _evField;

	public FieldBucketAggregator(final String serverField,final String dateField,final String evField) {
		_serverField = serverField;
		_dateField = dateField;
		_evField = evField;
	}

	@Override
	public ServerLoadMap init(Object batchId, TridentCollector collector) {
		return new ServerLoadMap();
	}

	@Override
    public void aggregate(ServerLoadMap state, TridentTuple tuple, TridentCollector collector) {
		final String server = tuple.getStringByField(_serverField);
		final String datetime = tuple.getStringByField(_dateField);
		final String ev = tuple.getStringByField(_evField);

		if( server == null || datetime == null || ev == null ) {
			return;
		}

		Date date;
		try {
			date = DateUtils.parseDate(datetime,TopologyConstant.INPUT_DATETIME_FORMAT);
		} catch( ParseException e ) {
			log.warn("Bad datetime format: " + datetime);
			return;
		}

		state.count(server,DateUtils.truncate(date,Calendar.SECOND),ev);
	}

	@Override
    public void complete(ServerLoadMap state, TridentCollector collector) {
		Gson gson = new GsonBuilder().setDateFormat(TopologyConstant.STD_DATETIME_FORMAT).create();
		List<String[]> reports = state.toReports(gson,TopologyConstant.STD_DATETIME_FORMAT);
		if( reports == null ) {
			return;
		}

		for(String[] report: reports) {
			collector.emit(new Values(report[0],report[1],report[2]));
		}
	}
}

