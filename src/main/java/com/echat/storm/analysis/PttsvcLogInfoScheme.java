package com.echat.storm.analysis;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.spout.SchemeAsMultiScheme;


public class PttsvcLogInfoScheme extends Scheme {
	public static class PttsvcLogInfoSchemaConstrants {
		public static final String APP_FIELD = "app";
		public static final String DATETIME_FIELD = "datetime";
		public static final String LEVEL_FIELD = "level";
		public static final String CONTENT_FIELD = "content";

		public static final String PATTERN = "^(\\w+)\\s+(\\d{4}[/:\\.\\-\\\\]\\d{2}[/:\\.\\-\\\\]\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\.\\d+\\s(\\w+)[^\"]*\"([^\"]+)\"";

	}


	private static final Logger LOGGER = LoggerFactory.getLogger(PttsvcLogInfoScheme.class);
	private PatternCompiler compiler = new Perl5Compiler();
	private Pattern pattern = compiler.compile(PttsvcLogInfoSchemaConstrants.PATTERN);

	public List<Object> deserialize(byte[] bytes) {
		String line = new String(bytes,"UTF-8");
		PatternMatcher pm = new Perl5Matcher();
		if( pm.contains(line,pattern) ) {
			MatchResult mr = pm.getMatch();
			String app = mr.group(1);
			String datetime = mr.group(2);
			String level = mr.group(3);
			String content = mr.group(4);

			return Arrays.asList(app,datetime,level,content);
		} else {
			return null;
		}
	}

	public Fields getOutputFields() {
		return new Fields(
				PttsvcLogInfoSchemaConstrants.APP_FIELD,
				PttsvcLogInfoSchemaConstrants.DATETIME_FIELD,
				PttsvcLogInfoSchemaConstrants.LEVEL_FIELD,
				PttsvcLogInfoSchemaConstrants.CONTENT_FIELD);
	}
}
