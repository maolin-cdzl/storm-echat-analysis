package com.echat.storm.analysis.constant;

public class HBaseConstant {
	// user action table
	public static final String USER_ACTION_TABLE = "user_action";
	public static final byte[] COLUMN_FAMILY_LOG = "l".getBytes();

	// broken history table
	public static final String BROKEN_HISTORY_TABLE = "broken_history";

	// columns
	public static final byte[] COLUMN_SERVER = FieldConstant.SERVER_FIELD.getBytes();
	public static final byte[] COLUMN_DATETIME = FieldConstant.DATETIME_FIELD.getBytes();
	public static final byte[] COLUMN_EVENT = FieldConstant.EVENT_FIELD.getBytes();
	public static final byte[] COLUMN_UID = FieldConstant.UID_FIELD.getBytes();
	public static final byte[] COLUMN_GID = FieldConstant.GID_FIELD.getBytes();
	public static final byte[] COLUMN_COMPANY = FieldConstant.COMPANY_FIELD.getBytes();
	public static final byte[] COLUMN_AGENT = FieldConstant.AGENT_FIELD.getBytes();
	public static final byte[] COLUMN_CTX = FieldConstant.CTX_FIELD.getBytes();
	public static final byte[] COLUMN_IP = FieldConstant.IP_FIELD.getBytes();
	public static final byte[] COLUMN_DEVICE = FieldConstant.DEVICE_FIELD.getBytes();
	public static final byte[] COLUMN_DEVICE_ID = FieldConstant.DEVICE_ID_FIELD.getBytes();
	public static final byte[] COLUMN_VERSION = FieldConstant.VERSION_FIELD.getBytes();
	public static final byte[] COLUMN_IMSI = FieldConstant.IMSI_FIELD.getBytes();
	public static final byte[] COLUMN_EXPECT_PAYLOAD = FieldConstant.EXPECT_PAYLOAD_FIELD.getBytes();
	public static final byte[] COLUMN_TARGET = FieldConstant.TARGET_FIELD.getBytes();
	public static final byte[] COLUMN_TARGET_GOT = FieldConstant.TARGET_GOT_FIELD.getBytes();
	public static final byte[] COLUMN_TARGET_DENT = FieldConstant.TARGET_DENT_FIELD.getBytes();
	public static final byte[] COLUMN_COUNT = FieldConstant.COUNT_FIELD.getBytes();
	public static final byte[] COLUMN_SW = FieldConstant.SW_FIELD.getBytes();
	public static final byte[] COLUMN_VALUE = FieldConstant.VALUE_FIELD.getBytes();


	public static final byte[] COLUMN_OFFLINE_TIME = FieldConstant.OFFLINE_TIME_FIELD.getBytes();

}
