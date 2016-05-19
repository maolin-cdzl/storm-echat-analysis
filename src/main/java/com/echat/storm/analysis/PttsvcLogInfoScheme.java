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
 
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.spout.SchemeAsMultiScheme;

import java.util.Arrays;
import java.util.List;
import java.io.UnsupportedEncodingException;

public class PttsvcLogInfoScheme implements Scheme {

	private static final String PATTERN = "^(\\w+)\\s+(\\d{4}[/:\\.\\-\\\\]\\d{2}[/:\\.\\-\\\\]\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\.\\d+\\s(\\w+)[^\"]*\"([^\"]+)\"";

	private static final Logger log = LoggerFactory.getLogger(PttsvcLogInfoScheme.class);
	private Pattern pattern = null;

	public PttsvcLogInfoScheme() throws MalformedPatternException {
		PatternCompiler compiler = new Perl5Compiler();
		this.pattern = compiler.compile(PATTERN);
	}

	public List<Object> deserialize(byte[] bytes) {
		String line = null;
		try {
			line = new String(bytes,"UTF-8");
		} catch( UnsupportedEncodingException e) {
			log.warn("Unsupport tuple recved");
			return null;
		}

		PatternMatcher pm = new Perl5Matcher();
		if( pm.contains(line,pattern) ) {
			MatchResult mr = pm.getMatch();
			String app = mr.group(1);
			String datetime = mr.group(2);
			String level = mr.group(3);
			String content = mr.group(4);

			log.debug("Kafka tuple: " + app + "\t" + datetime + "\t" + level + "\t" + content);
			return new Values(app,datetime,level,content);
		} else {
			return null;
		}
	}

	public Fields getOutputFields() {
		return new Fields(
				FieldsConstrants.APP_FIELD,
				FieldsConstrants.DATETIME_FIELD,
				FieldsConstrants.LEVEL_FIELD,
				FieldsConstrants.CONTENT_FIELD);
	}
}
