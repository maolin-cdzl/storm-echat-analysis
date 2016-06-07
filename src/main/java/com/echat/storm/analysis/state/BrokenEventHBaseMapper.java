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
import com.echat.storm.analysis.types.BrokenEvent;
import com.echat.storm.analysis.utils.ToBytesUtils;

public class BrokenEventHBaseMapper implements TridentHBaseMapper {
	//private static final Logger logger = LoggerFactory.getLogger(BrokenEventHBaseMapper.class);
	private ToBytesUtils bytesUtils = new ToBytesUtils();

	@Override
	public byte[] rowKey(TridentTuple tuple) {
		BrokenEvent broken = BrokenEvent.fromTuple(tuple);
		byte[] userPart = bytesUtils.stringToMD5Bytes(broken.uid);
		byte[] tsPart = ToBytesUtils.longToBytes(broken.getDate().getTime());
		byte[] companyPart = ToBytesUtils.stringToHashBytes(broken.company);
		byte[] svrPart = ToBytesUtils.stringToHashBytes(broken.server);

		byte[] key = ToBytesUtils.concatBytes(userPart,tsPart,companyPart,svrPart);

		return key;
	}

	@Override
    public ColumnList columns(TridentTuple tuple) {
		BrokenEvent broken = BrokenEvent.fromTuple(tuple);
		ColumnList columns = new ColumnList();

		// must exists column
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,broken.server.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,broken.datetime.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,broken.uid.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_COMPANY,broken.company.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_AGENT,broken.agent.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_OFFLINE_TIME,ToBytesUtils.longToBytes(broken.offtime));
		if( broken.ctx != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_CTX,broken.ctx.getBytes());
		}
		if( broken.ip != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IP,broken.ip.getBytes());
		}
		if( broken.device != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE,broken.device.getBytes());
		}
		if( broken.devid != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DEVICE_ID,broken.devid.getBytes());
		}
		if( broken.version != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_VERSION,broken.version.getBytes());
		}
		if( broken.imsi != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_IMSI,broken.imsi.getBytes());
		}
		return columns;
	}
}


