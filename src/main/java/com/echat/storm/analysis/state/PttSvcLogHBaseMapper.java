package com.echat.storm.analysis.state;

import storm.hbase.common.ColumnList;
import storm.hbase.bolt.mapper.HBaseMapper;

import com.echat.storm.analysis.constant.HBaseConstant;

class PttSvcLogHBaseMapper implements HBaseMapper {
	private ByteBuffer buffer = null;

	private longToBytes(Long v) {
		if( buffer == null ) {
			buffer = ByteBuffer.allocate(Long.BYTES);
		}
		buffer.putLong(v);
		return buffer.array();
	}

	@Override
	public byte[] rowKey(Tuple tuple) {
	}

	@Override
    public ColumnList columns(Tuple tuple) {
		PttSvcLog log = PttSvcLog.fromTuple(tuple);
		ColumnList columns = new ColumnList();

		// must exists column
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_SERVER,log.server.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_DATETIME,log.datetime.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_TIMESTAMP,longToBytes(log.timestamp));
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_LEVEL,log.level.getBytes());
		columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_CONTENT,log.content.getBytes());

		if( log.event != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_EVENT,log.event.getBytes());
		} else {
			// if no event,those below also unexists.
			return columns;
		}
		if( log.uid != null ) {
			columns.addColumn(HBaseConstant.COLUMN_FAMILY_LOG,HBaseConstant.COLUMN_UID,log.uid.getBytes());
		}
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

