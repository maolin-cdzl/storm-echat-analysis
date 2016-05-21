package com.echat.test;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.apache.oro.text.regex.PatternMatcherInput;

public class Test {
	public static void main(String args[]) throws MalformedPatternException  {
		testGrouping();
		testSearch();
	}

	private static void testGrouping()  throws MalformedPatternException {
		System.out.println("......testGrouping");
		String PATTERN = "^(\\w+)\\s+(\\d{4}[/:\\.\\-\\\\]\\d{2}[/:\\.\\-\\\\]\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\.\\d+\\s(\\w+)[^\"]*\"([^\"]+)\"";
		String line = "pttsvc004 2016/05/19 16:25:41.1853981196219	WARNING [commandhandler.cpp L: 145]	\"HANLE(5806) fwfilter: ptt.rr.RequestMic\"";

		System.out.println(PATTERN);

		PatternCompiler compiler = new Perl5Compiler();
		Pattern pattern = compiler.compile(PATTERN);
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

	private static void testSearch()  throws MalformedPatternException {
		System.out.println("......testSearch");
		String PATTERN = "(\\w+)=\\(([^\\)]*)\\)";
		//String line = "RELOGIN uid=(10086693) address=(27.128.6.165:50044) version=(0) platform=(brew) device=(ZTE G180) expect_payload=(103) os=(BREW) esn=() meid=(a000004e0cad3c) context=(std)  imsi=(460030762270753)";
		String line = "LOGIN FAILED result=(-2:超出服务期,请充值) account=(16807310148) address=(222.246.185.57:59416) version=(0) platform=(brew) device=(ZTE G500) expect_payload=(103)";

		System.out.println(PATTERN);

		PatternCompiler compiler = new Perl5Compiler();
		Pattern pattern = compiler.compile(PATTERN);
		PatternMatcher pm = new Perl5Matcher();
		PatternMatcherInput matcherInput = new PatternMatcherInput(line);  

		while( pm.contains(matcherInput,pattern) ) {
			MatchResult mr = pm.getMatch();  
			System.out.println(mr.group(1) + "=" + mr.group(2));  
		}  
	}
}

