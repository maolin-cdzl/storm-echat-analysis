package com.echat.test;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

public class Test {
	//private static final String PATTEN = "^(\\w+)\\s+(\\d{4}[/:-]\\d{2][/:-]\\d{2} \\d{2}[/:-]\\d{2}[/:-]\\d{2}[/:-])\\.\\d+\\s(\\w+)[^\"]*\"([^\"]+)\"";
	private static final String PATTEN = "^(\\w+)\\s+(\\d{4}[/:\\.\\-\\\\]\\d{2}[/:\\.\\-\\\\]\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\.\\d+\\s(\\w+)[^\"]*\"([^\"]+)\"";

	//private static final String line = "pttsvc004 2016-05-19 16:25:39.1853979883830	INFO [commandprocesser.cpp L: 564]	\"RELEASE MIC uid=(10126717) gid=(25649)\"";
	private static final String line = "pttsvc004 2016/05/19 16:25:41.1853981196219	WARNING [commandhandler.cpp L: 145]	\"HANLE(5806) fwfilter: ptt.rr.RequestMic\"";
	public static void main(String args[]) throws MalformedPatternException  {
		System.out.println(PATTEN);

		PatternCompiler compiler = new Perl5Compiler();
		Pattern pattern = null;
		try {
			pattern = compiler.compile(PATTEN);
		} catch(MalformedPatternException e) {
			throw e;
		}

		PatternMatcher pm = new Perl5Matcher();
		if( pm.contains(line,pattern) ) {
			MatchResult mr = pm.getMatch();
			String app = mr.group(1);
			String datetime = mr.group(2);
			String level = mr.group(3);
			String content = mr.group(4);

			System.out.println(app);
			System.out.println(datetime);
			System.out.println(level);
			System.out.println(content);
		} else {
			System.out.println("Not contain");
		}
	}
}

