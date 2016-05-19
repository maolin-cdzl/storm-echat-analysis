package com.echat.storm.analysis;

import java.util.regex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.spout.SchemeAsMultiScheme;


public class PttsvcLogInfoSchema extends Scheme {
	public static class PttsvcLogInfoSchemaConstrants {
		public static final String APP_FIELD = "app";
		public static final String DATETIME_FIELD = "datetime";
		public static final String ACTION_FIELD = "action";
		public static final String CONTENT_FIELD = "content";
	}


	private static final Logger LOGGER = LoggerFactory.getLogger(PttsvcLogInfoSchema.class);


	public List<Object> deserialize(byte[] bytes) {
	}

	public Fields getOutputFields() {
		return new Fields(
				PttsvcLogInfoSchemaConstrants.APP_FIELD,
				PttsvcLogInfoSchemaConstrants.DATETIME_FIELD
				PttsvcLogInfoSchemaConstrants.ACTION_FIELD,
				PttsvcLogInfoSchemaConstrants.CONTENT_FIELD);
	}
}
