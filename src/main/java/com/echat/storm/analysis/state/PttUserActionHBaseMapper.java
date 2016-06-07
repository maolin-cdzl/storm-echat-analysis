package com.echat.storm.analysis.state;

import java.util.Date;
import java.text.ParseException;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.tuple.TridentTuple;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.echat.storm.analysis.constant.*;
import com.echat.storm.analysis.types.PttUserActionLog;
import com.echat.storm.analysis.utils.ToBytesUtils;

public class PttUserActionHBaseMapper implements TridentHBaseMapper {
	//private static final Logger logger = LoggerFactory.getLogger(PttUserActionHBaseMapper.class);
	
	private ToBytesUtils bytesUtils = new ToBytesUtils();

	@Override
	public byte[] rowKey(TridentTuple tuple) {
		PttUserActionLog log = PttUserActionLog.fromTuple(tuple);
		
		byte[] userPart = bytesUtils.stringToMD5Bytes(log.uid);
		byte[] tsPart = ToBytesUtils.longToBytes(log.getDate().getTime());
		byte[] evPart = ToBytesUtils.stringToHashBytes(log.event);
		byte[] svrPart = ToBytesUtils.stringToHashBytes(log.server);

		byte[] key = ToBytesUtils.concatBytes(userPart,tsPart,evPart,svrPart);

		return key;
	}

	@Override
    public ColumnList columns(TridentTuple tuple) {
		PttUserActionLog log = PttUserActionLog.fromTuple(tuple);
		ColumnList columns = new ColumnList();

		// must exists column
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,log.server.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,log.datetime.getBytes());
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

