package com.echat.test;

import java.util.Date;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.ParseException;
import org.apache.commons.lang.time.DateUtils;

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
		//testGrouping();
		//testSearch();
		testRowKey();
	}

	private static void testGrouping()  throws MalformedPatternException {
		System.out.println("......testGrouping");
		String PATTERN = "^(\\w+)\\s+(\\d{4}[/:\\.\\-\\\\]\\d{2}[/:\\.\\-\\\\]\\d{2}\\s\\d{2}:\\d{2}:\\d{2})\\.(\\d+)\\s(\\w+)[^\"]*\"([^\"]+)\"";
		String line = "pttsvc004 2016/05/19 16:25:41.1853981196219	WARNING [commandhandler.cpp L: 145]	\"HANLE(5806) fwfilter: ptt.rr.RequestMic\"";

		System.out.println(PATTERN);

		PatternCompiler compiler = new Perl5Compiler();
		Pattern pattern = compiler.compile(PATTERN);
		PatternMatcher pm = new Perl5Matcher();

		if( pm.contains(line,pattern) ) {
			MatchResult mr = pm.getMatch();
			String app = mr.group(1);
			String datetime = mr.group(2);
			String timestamp = mr.group(3);
			String level = mr.group(4);
			String content = mr.group(5);

			System.out.println(app);
			System.out.println(datetime);
			System.out.println(timestamp);
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

	private static void testRowKey() {
		final String[] INPUT_DATETIME_FORMAT = new String[] { 
			"yyyy-MM-dd HH:mm:ss.SSS",
			"yyyy/MM/dd HH:mm:ss",
			"yyyy-MM-dd HH:mm:ss"
		};
		final String uid = "18900001111";
		final String event = "LOGIN";
		final String datetime = "2016-06-06 21:56:33";
		final Long timestamp = 13434999L;
		final String server = "pttsvc001";

		Date date;
		try {
			date = DateUtils.parseDate(datetime,INPUT_DATETIME_FORMAT);
		} catch( ParseException e ) {
			return;
		}

		// because current log time has no milliseconds!
		Long ts = date.getTime() + (timestamp % 1000);
		
		byte[] userPart = standardUid(uid);
		byte[] tsPart = longToBytes(ts);
		byte[] evPart = standardString(event);
		byte[] svrPart = standardString(server);

		System.out.println("userPart length: " + userPart.length);
		System.out.println("tsPart length: " + tsPart.length);
		System.out.println("evPart length: " + evPart.length);
		System.out.println("svrPart length: " + svrPart.length);

		byte[] key = new byte[userPart.length + tsPart.length + evPart.length + svrPart.length];
		System.arraycopy(userPart, 0, key, 0, userPart.length);
		System.arraycopy(tsPart, 0, key, userPart.length, tsPart.length);
		System.arraycopy(evPart,0,key,userPart.length + tsPart.length,evPart.length);
		System.arraycopy(svrPart,0,key,userPart.length + tsPart.length + evPart.length,svrPart.length);

		System.out.println("key length: " + key.length);
		return;
	}

	static private byte[] longToBytes(Long v) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
		buffer.putLong(v);
		return buffer.array();
	}

	static private byte[] intToByte(int v) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
		buffer.putInt(v);
		return buffer.array();
	}

	static private byte[] stringToMD5(final String str) {
		if( md == null ) {
			try {
				md = MessageDigest.getInstance("MD5");
			} catch(java.security.NoSuchAlgorithmException e) {
				throw new RuntimeException("MD5 not support");
			}
		}
		return md.digest(str.getBytes());
	}

	static private byte[] standardUid(final String uid) {
		if( md == null ) {
			try {
				md = MessageDigest.getInstance("MD5");
			} catch(java.security.NoSuchAlgorithmException e) {
				throw new RuntimeException("MD5 not support");
			}
		}
		md.reset();
		return md.digest(uid.getBytes());
	}

	static private byte[] standardString(final String str) {
		byte[] bytes_str = str.getBytes();
		int sum = 0;
		for(int i=0; i < bytes_str.length; i++) {
			sum += bytes_str[i];
		}
		return intToByte(sum);
	}

	static private MessageDigest md = null;
}

