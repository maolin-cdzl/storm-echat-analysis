package com.echat.storm.analysis.operation;

import storm.trident.tuple.TridentTuple;
import storm.trident.operation.BaseFilter;

import com.echat.storm.analysis.constant.FieldConstant;

public class LevelFilter extends BaseFilter {
	private final String level;

	public LevelFilter(String level) {
		this.level = level;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		return level.equals(tuple.getStringByField(FieldConstant.LEVEL_FIELD));
	}
}

