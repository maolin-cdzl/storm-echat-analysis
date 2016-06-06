package com.echat.storm.analysis.state;

import java.util.Date;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.ParseException;
import org.apache.commons.lang.time.DateUtils;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.PttUserActionLog;

public class PttUserActionHBaseMapper implements TridentHBaseMapper {
	private static final Logger log = LoggerFactory.getLogger(PttUserActionHBaseMapper.class);
	private MessageDigest md = null;

	private byte[] longToBytes(long v) {
		ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / Byte.SIZE);
		buffer.putLong(v);
		return buffer.array();
	}

	private byte[] intToByte(int v) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
		buffer.putInt(v);
		return buffer.array();
	}

	private byte[] standardUid(final String uid) {
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

	private byte[] standardString(final String str) {
		byte[] bytes_str = str.getBytes();
		int sum = 0;
		for(int i=0; i < bytes_str.length; i++) {
			sum += bytes_str[i];
		}
		return intToByte(sum);
	}

	@Override
	public byte[] rowKey(TridentTuple tuple) {
		final String uid = tuple.getStringByField(FieldConstant.UID_FIELD);
		final String event = tuple.getStringByField(FieldConstant.EVENT_FIELD);
		final String datetime = tuple.getStringByField(FieldConstant.DATETIME_FIELD);
		final String server = tuple.getStringByField(FieldConstant.SERVER_FIELD);
		final Long timestamp = tuple.getLongByField(FieldConstant.TIMESTAMP_FIELD);

		Date date;
		try {
			date = DateUtils.parseDate(datetime,TopologyConstant.INPUT_DATETIME_FORMAT);
		} catch( ParseException e ) {
			log.error("Bad datetime format: " + datetime);
			return null;
		}

		// because current log time has no milliseconds!
		Long ts = date.getTime() + (timestamp % 1000);
		
		byte[] userPart = standardUid(uid);
		byte[] tsPart = longToBytes(ts);
		byte[] evPart = standardString(event);
		byte[] svrPart = standardString(server);

		byte[] key = new byte[userPart.length + tsPart.length + evPart.length + svrPart.length];
		System.arraycopy(userPart, 0, key, 0, userPart.length);
		System.arraycopy(tsPart, 0, key, userPart.length, tsPart.length);
		System.arraycopy(evPart,0,key,userPart.length + tsPart.length,evPart.length);
		System.arraycopy(svrPart,0,key,userPart.length + tsPart.length + evPart.length,svrPart.length);

		return key;
	}

	@Override
    public ColumnList columns(TridentTuple tuple) {
		PttUserActionLog log = PttUserActionLog.fromTuple(tuple);
		ColumnList columns = new ColumnList();

		// must exists column
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,log.server.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,log.datetime.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TIMESTAMP,longToBytes(log.timestamp));
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EVENT,log.event.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,log.uid.getBytes());

		if( log.gid != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_GID,log.gid.getBytes());
		}
		if( log.company!= null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,log.company.getBytes());
		}
		if( log.agent != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,log.agent.getBytes());
		}
		if( log.result != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_RESULT,log.result.getBytes());
		}
		if( log.ctx != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_CTX,log.ctx.getBytes());
		}
		if( log.ip != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IP,log.ip.getBytes());
		}
		if( log.device != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE,log.device.getBytes());
		}
		if( log.devid != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE_ID,log.devid.getBytes());
		}
		if( log.version != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VERSION,log.version.getBytes());
		}
		if( log.imsi != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IMSI,log.imsi.getBytes());
		}
		if( log.expect_payload != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EXPECT_PAYLOAD,log.expect_payload.getBytes());
		}
		if( log.target != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET,log.target.getBytes());
		}
		if( log.target_got != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET_GOT,log.target_got.getBytes());
		}
		if( log.target_dent != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TARGET_DENT,log.target_dent.getBytes());
		}
		if( log.count != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COUNT,log.count.getBytes());
		}
		if( log.sw != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SW,log.sw.getBytes());
		}
		if( log.value != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VALUE,log.value.getBytes());
		}
		return columns;
	}
}

