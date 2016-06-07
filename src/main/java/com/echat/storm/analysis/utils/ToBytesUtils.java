package com.echat.storm.analysis.utils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

public class ToBytesUtils implements Serializable {
	private MessageDigest md = null;

	static public byte[] longToBytes(long v) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
		buffer.putLong(v);
		return buffer.array();
	}

	static public byte[] intToByte(int v) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
		buffer.putInt(v);
		return buffer.array();
	}

	static public byte[] stringToHashBytes(final String str) {
		int sum = 0;
		if( str != null ) {
			byte[] bytes_str = str.getBytes();
			for(int i=0; i < bytes_str.length; i++) {
				sum += bytes_str[i];
			}
		}
		return intToByte(sum);
	}

	static public byte[] concatBytes(byte[] ... byteArr) {
		int length = 0;
		for(byte[] o : byteArr) {
			length += o.length;
		}

		byte[] mix = new byte[length];
		length = 0;
		for(byte[] o : byteArr) {
			System.arraycopy(o, 0, mix, length, o.length);
			length += o.length;
		}
		return mix;
	}

	public byte[] stringToMD5Bytes(final String uid) {
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

}

